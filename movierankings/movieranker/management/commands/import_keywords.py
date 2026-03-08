import csv
import json
import ast
import time
from pathlib import Path
from typing import List, Dict, Tuple, Set

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction, connection

from movieranker.models import Movie, Keyword, MovieKeyword


def parse_json_array(s: str):
    if not s:
        return []
    s = s.strip()
    if s in ("", "[]", "null", "NULL", "NaN"):
        return []
    try:
        out = json.loads(s)
        return out if isinstance(out, list) else []
    except Exception:
        pass
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
    help = "Import keywords.csv into Keyword and MovieKeyword (batched, fast)."

    def add_arguments(self, parser):
        parser.add_argument("--path", required=True, help="Path to keywords.csv")
        parser.add_argument("--batch-size", type=int, default=10000,
                            help="Rows (movies) per batch; try 10k for speed.")
        parser.add_argument("--limit", type=int, default=0,
                            help="Import only first N rows (testing).")
        parser.add_argument("--sqlite-fast", action="store_true",
                            help="Apply PRAGMAs to speed SQLite during import (dev only).")
        parser.add_argument("--atomic", dest="atomic", action="store_true", default=True,
                            help="Wrap the entire import in one big transaction (default).")
        parser.add_argument("--no-atomic", dest="atomic", action="store_false",
                            help="Disable global atomic; still atomic per batch.")
        parser.add_argument("--no-name-fallback", action="store_true",
                            help="Only deduplicate Keyword by tmdb_id (skip name fallback).")

    def handle(self, *args, **opts):
        path = Path(opts["path"])
        if not path.exists():
            raise CommandError(f"File not found: {path}")

        batch_size = int(opts["batch_size"])
        limit = int(opts["limit"])
        sqlite_fast = bool(opts["sqlite_fast"])
        global_atomic = bool(opts["atomic"])
        no_name_fallback = bool(opts["no_name_fallback"])

        # Optional SQLite perf tweaks
        if sqlite_fast and connection.vendor == "sqlite":
            with connection.cursor() as cur:
                cur.execute("PRAGMA journal_mode=WAL;")
                cur.execute("PRAGMA synchronous=NORMAL;")
                cur.execute("PRAGMA temp_store=MEMORY;")

        # Movie map
        self.stdout.write("Building movie id map…")
        tmdb_to_movie_id: Dict[int, int] = dict(
            Movie.objects.exclude(tmdb_id__isnull=True).values_list("tmdb_id", "id")
        )
        if not tmdb_to_movie_id:
            raise CommandError("No Movie rows found. Import movies_metadata.csv first.")

        # Warm Keyword caches
        self.stdout.write("Warming keyword caches…")
        keyword_by_tmdb: Dict[int, Keyword] = {
            tid: Keyword(id=kid, tmdb_id=tid, name=name)
            for tid, kid, name in Keyword.objects.exclude(tmdb_id__isnull=True).values_list("tmdb_id", "id", "name")
        }
        keyword_by_name: Dict[str, Keyword] = {}
        if not no_name_fallback:
            keyword_by_name = {
                name: Keyword(id=kid, tmdb_id=tid, name=name)
                for name, kid, tid in Keyword.objects.values_list("name", "id", "tmdb_id")
            }

        created_keywords_total = 0
        attempted_links_total = 0
        skipped_rows = 0

        def process_batch(rows: List[dict], bi: int):
            nonlocal created_keywords_total, attempted_links_total, skipped_rows

            t0 = time.perf_counter()

            # Phase 1: collect new keywords to insert for this batch
            to_insert_tmdb: Dict[int, str] = {}  # tmdb_id -> name
            to_insert_name: Set[str] = set()

            parsed: List[Tuple[int, List[dict]]] = []  # (movie_id, keywords list)

            for row in rows:
                tmdb_mid = as_int(row.get("id"))
                movie_id = tmdb_to_movie_id.get(tmdb_mid)
                if not movie_id:
                    skipped_rows += 1
                    continue

                items = parse_json_array(row.get("keywords"))
                parsed.append((movie_id, items))

                for kw in items:
                    k_tid = as_int(kw.get("id"))
                    k_name = (kw.get("name") or "").strip()
                    if not k_tid and not k_name:
                        continue
                    if k_tid:
                        if k_tid not in keyword_by_tmdb:
                            to_insert_tmdb.setdefault(k_tid, k_name or "")
                    elif not no_name_fallback and k_name and k_name not in keyword_by_name:
                        to_insert_name.add(k_name)

            t1 = time.perf_counter()

            # Phase 2: bulk create new keywords (once per batch)
            new_keywords = []
            for tid, nm in to_insert_tmdb.items():
                new_keywords.append(Keyword(tmdb_id=tid, name=nm or ""))
            if not no_name_fallback:
                for nm in to_insert_name:
                    new_keywords.append(Keyword(tmdb_id=None, name=nm))

            created_now = 0
            if new_keywords:
                Keyword.objects.bulk_create(new_keywords, ignore_conflicts=True)
                created_now = len(new_keywords)
                created_keywords_total += created_now

                # Refresh caches for attempted inserts
                if to_insert_tmdb:
                    for k in Keyword.objects.filter(tmdb_id__in=list(to_insert_tmdb.keys())).only("id", "tmdb_id", "name"):
                        keyword_by_tmdb[k.tmdb_id] = k
                        if not no_name_fallback and k.name and k.name not in keyword_by_name:
                            keyword_by_name[k.name] = k
                if not no_name_fallback and to_insert_name:
                    for k in Keyword.objects.filter(name__in=list(to_insert_name)).only("id", "tmdb_id", "name"):
                        if k.tmdb_id is not None:
                            keyword_by_tmdb[k.tmdb_id] = k
                        keyword_by_name[k.name] = k

            t2 = time.perf_counter()

            # Phase 3: build MovieKeyword links, bulk_create once
            links_to_create: List[MovieKeyword] = []
            for movie_id, items in parsed:
                for kw in items:
                    k_tid = as_int(kw.get("id"))
                    k_name = (kw.get("name") or "").strip()
                    keyword = None
                    if k_tid and k_tid in keyword_by_tmdb:
                        keyword = keyword_by_tmdb[k_tid]
                    elif not no_name_fallback and k_name and k_name in keyword_by_name:
                        keyword = keyword_by_name[k_name]
                    else:
                        continue  # failed to resolve; should be rare if Phase 2 worked

                    links_to_create.append(MovieKeyword(movie_id=movie_id, keyword_id=keyword.id))

            attempted_links_total += len(links_to_create)

            with transaction.atomic():
                if links_to_create:
                    MovieKeyword.objects.bulk_create(links_to_create, ignore_conflicts=True)

            t3 = time.perf_counter()

            self.stdout.write(
                f"Batch {bi:>3}: movies={len(rows):>5} | "
                f"parse={t1 - t0:>5.2f}s, keywords={t2 - t1:>5.2f}s (+{created_now}), "
                f"links={t3 - t2:>5.2f}s (attempted {len(links_to_create)})"
            )

        def iterate(reader):
            batch = []
            for i, row in enumerate(reader, start=1):
                if limit and i > limit:
                    break
                batch.append(row)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        # Main
        t_start = time.perf_counter()
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            required = {"id", "keywords"}
            if not required.issubset(reader.fieldnames or []):
                raise CommandError(f"Unexpected columns in {path.name}: {reader.fieldnames}")

            if global_atomic:
                with transaction.atomic():
                    for bi, rows in enumerate(iterate(reader), start=1):
                        process_batch(rows, bi)
            else:
                for bi, rows in enumerate(iterate(reader), start=1):
                    process_batch(rows, bi)

        t_end = time.perf_counter()
        self.stdout.write(self.style.SUCCESS(
            f"Done in {t_end - t_start:.2f}s. "
            f"Keywords created: {created_keywords_total}, "
            f"Links attempted: {attempted_links_total}, "
            f"Rows without movie: {skipped_rows}"
        ))
