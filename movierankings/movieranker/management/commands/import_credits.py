# movies/management/commands/import_credits.py
import csv
import json
import ast
import time
from pathlib import Path
from typing import List, Dict, Tuple, Set

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction, connection

from movieranker.models import Movie, Person, MovieCredit, CreditRole


def parse_json_array(s: str):
    """
    Parse column that should be a JSON array (cast/crew).
    Fast path: json.loads; Fallback: ast.literal_eval for single-quoted literals.
    """
    if not s:
        return []
    s = s.strip()
    if s in ("", "[]", "null", "NULL", "NaN"):
        return []
    # Fast path
    try:
        out = json.loads(s)
        return out if isinstance(out, list) else []
    except Exception:
        pass
    # Only try literal_eval if it "looks" like a Python literal
    if s[0] in "[{" and "'" in s and '"' not in s:
        try:
            out = ast.literal_eval(s)
            return out if isinstance(out, list) else []
        except Exception:
            return []
    return []


def as_int(x):
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nan", "null"}:
        return None
    try:
        return int(s)
    except Exception:
        try:
            return int(float(s))
        except Exception:
            return None


class Command(BaseCommand):
    help = "Optimized import of credits.csv (cast & crew) → Person & MovieCredit (batched)."

    def add_arguments(self, parser):
        parser.add_argument("--path", required=True, help="Path to credits.csv")
        parser.add_argument("--batch-size", type=int, default=5000,
                            help="Movies (rows) per batch; try 5000–10000 for speed.")
        parser.add_argument("--limit", type=int, default=0,
                            help="Optional: import only first N rows (for testing).")
        parser.add_argument("--sqlite-fast", action="store_true",
                            help="Apply PRAGMAs to speed SQLite imports (dev only).")
        parser.add_argument("--atomic", dest="atomic", action="store_true", default=True,
                            help="Wrap the entire import in one big transaction (default).")
        parser.add_argument("--no-atomic", dest="atomic", action="store_false",
                            help="Disable global atomic; still atomic per batch.")
        parser.add_argument("--no-name-fallback", action="store_true",
                            help="Only deduplicate Person by tmdb_id (fewer name collisions, faster).")

    def handle(self, *args, **opts):
        path = Path(opts["path"])
        if not path.exists():
            raise CommandError(f"File not found: {path}")

        batch_size = int(opts["batch_size"])
        limit = int(opts["limit"])
        sqlite_fast = bool(opts["sqlite_fast"])
        global_atomic = bool(opts["atomic"])
        no_name_fallback = bool(opts["no_name_fallback"])

        # Optional SQLite performance PRAGMAs (dev only)
        if sqlite_fast and connection.vendor == "sqlite":
            with connection.cursor() as cur:
                # WAL + relaxed sync dramatically improve bulk write throughput
                cur.execute("PRAGMA journal_mode=WAL;")
                cur.execute("PRAGMA synchronous=NORMAL;")
                cur.execute("PRAGMA temp_store=MEMORY;")
                # cur.execute("PRAGMA mmap_size=30000000000;")  # if supported

        # Preload a fast map for Movies: tmdb_id -> pk
        self.stdout.write("Building movie id map…")
        tmdb_to_movie_id: Dict[int, int] = dict(
            Movie.objects.exclude(tmdb_id__isnull=True).values_list("tmdb_id", "id")
        )
        if not tmdb_to_movie_id:
            raise CommandError("No Movie rows found. Import movies_metadata.csv first.")

        # Warm caches for Persons
        self.stdout.write("Warming person caches…")
        person_by_tmdb: Dict[int, Person] = {
            tid: Person(id=pid, tmdb_id=tid, name=name)
            for tid, pid, name in Person.objects.exclude(tmdb_id__isnull=True).values_list("tmdb_id", "id", "name")
        }
        person_by_name: Dict[str, Person] = {}
        if not no_name_fallback:
            person_by_name = {
                name: Person(id=pid, tmdb_id=tmdb, name=name)
                for name, pid, tmdb in Person.objects.values_list("name", "id", "tmdb_id")
            }

        created_people_total = 0
        attempted_credits_total = 0
        skipped_rows = 0

        def process_batch(rows: List[dict], batch_index: int):
            """
            Process a slice of CSV rows (movies). Two-phase:
              (1) scan to collect unique Persons to insert (minimize DB writes)
              (2) bulk-create Persons, refresh caches
              (3) build MovieCredit rows and bulk_create once
            """
            nonlocal created_people_total, attempted_credits_total, skipped_rows

            t0 = time.perf_counter()

            # Phase 1: collect new persons needed
            to_insert_tmdb: Dict[int, str] = {}   # tmdb_id -> name
            to_insert_name: Set[str] = set()      # names for persons with no tmdb_id

            parsed_rows: List[Tuple[int, List[dict], List[dict]]] = []  # (movie_id, cast, crew)

            for row in rows:
                tmdb_movie_id = as_int(row.get("id"))
                movie_id = tmdb_to_movie_id.get(tmdb_movie_id)
                if not movie_id:
                    skipped_rows += 1
                    continue

                cast_items = parse_json_array(row.get("cast"))
                crew_items = parse_json_array(row.get("crew"))
                parsed_rows.append((movie_id, cast_items, crew_items))

                # Collect needed persons for cast
                for member in cast_items:
                    p_tmdb = as_int(member.get("id"))
                    p_name = (member.get("name") or "").strip()
                    if not p_name and not p_tmdb:
                        continue
                    if p_tmdb:
                        if p_tmdb not in person_by_tmdb:
                            # Only stage insert if we don't already have this tmdb_id
                            # If we also have a name fallback cache, prefer tmdb anchoring
                            to_insert_tmdb.setdefault(p_tmdb, p_name or "")
                    elif not no_name_fallback and p_name and p_name not in person_by_name:
                        to_insert_name.add(p_name)

                # Collect needed persons for crew
                for member in crew_items:
                    p_tmdb = as_int(member.get("id"))
                    p_name = (member.get("name") or "").strip()
                    if not p_name and not p_tmdb:
                        continue
                    if p_tmdb:
                        if p_tmdb not in person_by_tmdb:
                            to_insert_tmdb.setdefault(p_tmdb, p_name or "")
                    elif not no_name_fallback and p_name and p_name not in person_by_name:
                        to_insert_name.add(p_name)

            t1 = time.perf_counter()

            # Phase 2: bulk-create new persons once per batch
            new_people = []
            for tid, nm in to_insert_tmdb.items():
                new_people.append(Person(tmdb_id=tid, name=nm or ""))
            if not no_name_fallback:
                for nm in to_insert_name:
                    new_people.append(Person(tmdb_id=None, name=nm))

            created_now = 0
            if new_people:
                Person.objects.bulk_create(new_people, ignore_conflicts=True)
                created_now = len(new_people)
                created_people_total += created_now

                # Refresh caches only for the set we attempted to insert
                if to_insert_tmdb:
                    for p in Person.objects.filter(tmdb_id__in=list(to_insert_tmdb.keys())).only("id", "tmdb_id", "name"):
                        person_by_tmdb[p.tmdb_id] = p
                        if not no_name_fallback and p.name and p.name not in person_by_name:
                            person_by_name[p.name] = p
                if not no_name_fallback and to_insert_name:
                    for p in Person.objects.filter(name__in=list(to_insert_name)).only("id", "tmdb_id", "name"):
                        if p.tmdb_id is not None:
                            person_by_tmdb[p.tmdb_id] = p
                        person_by_name[p.name] = p

            t2 = time.perf_counter()

            # Phase 3: build credits and bulk_create once
            credits_to_create: List[MovieCredit] = []

            for movie_id, cast_items, crew_items in parsed_rows:
                # Cast
                for member in cast_items:
                    p_tmdb = as_int(member.get("id"))
                    p_name = (member.get("name") or "").strip()
                    character = (member.get("character") or "").strip()
                    order = as_int(member.get("order"))

                    person = None
                    if p_tmdb and p_tmdb in person_by_tmdb:
                        person = person_by_tmdb[p_tmdb]
                    elif not no_name_fallback and p_name and p_name in person_by_name:
                        person = person_by_name[p_name]
                    else:
                        # Extremely rare edge case if creation failed; skip to avoid per-row DB hit
                        continue

                    credits_to_create.append(MovieCredit(
                        movie_id=movie_id,
                        person_id=person.id,
                        role=CreditRole.ACTOR,
                        job="",
                        department="",
                        character=character,
                        cast_order=order,
                    ))

                # Crew
                for member in crew_items:
                    p_tmdb = as_int(member.get("id"))
                    p_name = (member.get("name") or "").strip()
                    job = (member.get("job") or "").strip()
                    dept = (member.get("department") or "").strip()

                    person = None
                    if p_tmdb and p_tmdb in person_by_tmdb:
                        person = person_by_tmdb[p_tmdb]
                    elif not no_name_fallback and p_name and p_name in person_by_name:
                        person = person_by_name[p_name]
                    else:
                        continue

                    credits_to_create.append(MovieCredit(
                        movie_id=movie_id,
                        person_id=person.id,
                        role=CreditRole.CREW,
                        job=job,
                        department=dept,
                        character="",
                        cast_order=None,
                    ))

            attempted_credits_total += len(credits_to_create)

            # Bulk create credits once per batch inside a transaction
            with transaction.atomic():
                if credits_to_create:
                    MovieCredit.objects.bulk_create(credits_to_create, ignore_conflicts=True)

            t3 = time.perf_counter()

            self.stdout.write(
                f"Batch {batch_index:>3}: movies={len(rows):>5} | "
                f"parse={t1 - t0:>5.2f}s, people={t2 - t1:>5.2f}s (+{created_now}), "
                f"credits={t3 - t2:>5.2f}s (attempted {len(credits_to_create)})"
            )

        def iterate_rows(reader):
            """Yield in batches of dict rows (each row ≈ one movie)."""
            batch, count = [], 0
            for i, row in enumerate(reader, start=1):
                if limit and i > limit:
                    break
                batch.append(row)
                if len(batch) >= batch_size:
                    yield batch
                    count += 1
                    batch = []
            if batch:
                yield batch

        # Main import
        t_start = time.perf_counter()

        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            required = {"cast", "crew", "id"}
            if not required.issubset(reader.fieldnames or []):
                raise CommandError(f"Unexpected columns in {path.name}: {reader.fieldnames}")

            if global_atomic:
                with transaction.atomic():
                    for bi, batch_rows in enumerate(iterate_rows(reader), start=1):
                        process_batch(batch_rows, bi)
            else:
                for bi, batch_rows in enumerate(iterate_rows(reader), start=1):
                    process_batch(batch_rows, bi)

        t_end = time.perf_counter()

        self.stdout.write(self.style.SUCCESS(
            f"Done in {t_end - t_start:.2f}s. "
            f"People created: {created_people_total}, "
            f"Credits attempted: {attempted_credits_total}, "
            f"Rows without movie: {skipped_rows}"
        ))