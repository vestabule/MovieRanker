import csv
import json
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from movieranker.models import Movie, Genre, Company, Country, Language
    
import json, ast


def parse_json_array(value: str):
    """
    Parse a string that is supposed to be a JSON array of dicts, but may arrive
    single-quoted (Python-literal style) due to how the Kaggle CSV is saved.
    Returns [] on any failure.
    """
    if not value:
        return []
    s = value.strip()
    if s in ("", "[]", "null", "NULL", "NaN"):
        return []
    # Fast path: valid JSON
    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        pass

    # Fallback: Python literal (handles single quotes, etc.)
    try:
        parsed = ast.literal_eval(s)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        print(f"Failed to parse {value}")
        return []

def safe_int(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s in ("", "NaN", "null", "NULL"):
            return None
        return int(float(s))
    except Exception:
        return None

def safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s in ("", "NaN", "null", "NULL"):
            return None
        return float(s)
    except Exception:
        return None

def safe_date(x):
    if not x:
        return None
    s = str(x).strip()
    if s in ("", "NaN", "null", "NULL", "0"):
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


class Command(BaseCommand):
    help = "Import movies_metadata.csv into Movie and related lookup tables."

    def add_arguments(self, parser):
        parser.add_argument("--path", required=True, help="Path to movies_metadata.csv")
        parser.add_argument("--batch-size", type=int, default=1000, help="Rows per transaction batch")
        parser.add_argument("--limit", type=int, default=0, help="Optional: only import first N rows (for testing)")

    def handle(self, *args, **opts):
        path = Path(opts["path"])
        batch_size = int(opts["batch_size"])
        limit = int(opts["limit"])
        if not path.exists():
            raise CommandError(f"File not found: {path}")

        # In-memory caches to minimize DB queries
        genre_by_name = {g.name: g for g in Genre.objects.all()}
        genre_by_tmdb = {g.tmdb_id: g for g in Genre.objects.exclude(tmdb_id__isnull=True)}

        company_by_name = {c.name: c for c in Company.objects.all()}
        company_by_tmdb = {c.tmdb_id: c for c in Company.objects.exclude(tmdb_id__isnull=True)}

        country_by_code = {c.iso_3166_1: c for c in Country.objects.all()}
        lang_by_code = {l.iso_639_1: l for l in Language.objects.all()}

        to_create_movies = []
        rows_buffer = []  # keep original row for M2M processing
        created, updated, skipped = 0, 0, 0

        # We’ll stream the CSV, batching inserts and relations
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader, start=1):
                if limit and i > limit:
                    break

                # Just after reading each row
                # TODO: Remove these
                if i <= 3:
                    self.stdout.write(f"Row {i} genres raw: {row.get('genres')!r}")
                    self.stdout.write(f"Row {i} companies raw: {row.get('production_companies')!r}")
                    self.stdout.write(f"Row {i} languages raw: {row.get('spoken_languages')}")

                # Build Movie (without M2M yet)
                tmdb_id = safe_int(row.get("id"))

                # Some rows in the dataset can be malformed; skip if no valid TMDb id
                if not tmdb_id:
                    skipped += 1
                    continue

                imdb_id = (row.get("imdb_id") or "").strip() or None
                # Important text/numeric fields
                movie = Movie(
                    tmdb_id=tmdb_id,
                    imdb_id=imdb_id,
                    belongs_to_collection_raw=(row.get("belongs_to_collection") or "").strip(),
                    budget=safe_int(row.get("budget")),
                    genres_raw=(row.get("genres") or "").strip(),
                    homepage=(row.get("homepage") or "").strip(),
                    original_language_code=(row.get("original_language") or "").strip(),
                    original_title=(row.get("original_title") or "").strip(),
                    overview=(row.get("overview") or "").strip(),
                    popularity=safe_float(row.get("popularity")),
                    production_companies_raw=(row.get("production_companies") or "").strip(),
                    production_countries_raw=(row.get("production_countries") or "").strip(),
                    release_date=safe_date(row.get("release_date")),
                    revenue=safe_int(row.get("revenue")),
                    runtime=safe_float(row.get("runtime")),
                    spoken_languages_raw=(row.get("spoken_languages") or "").strip(),
                    status=(row.get("status") or "").strip(),
                    tagline=(row.get("tagline") or "").strip(),
                    title=(row.get("title") or "").strip(),
                    vote_average=safe_float(row.get("vote_average")),
                    vote_count=safe_int(row.get("vote_count")),
                )

                # Keep buffers for a batch flush
                to_create_movies.append(movie)
                rows_buffer.append(row)

                if len(to_create_movies) >= batch_size:
                    c, u = self._flush(
                        to_create_movies, rows_buffer,
                        genre_by_name, genre_by_tmdb,
                        company_by_name, company_by_tmdb,
                        country_by_code, lang_by_code
                    )
                    created += c
                    updated += u
                    to_create_movies.clear()
                    rows_buffer.clear()
                    self.stdout.write(self.style.NOTICE(f"Imported ~{created+updated} rows..."))

            # final flush
            if to_create_movies:
                c, u = self._flush(
                    to_create_movies, rows_buffer,
                    genre_by_name, genre_by_tmdb,
                    company_by_name, company_by_tmdb,
                    country_by_code, lang_by_code
                )
                created += c
                updated += u

        self.stdout.write(self.style.SUCCESS(
            f"Done. Created: {created}, Updated: {updated}, Skipped(bad TMDb id): {skipped}"
        ))

    @transaction.atomic
    def _flush(
        self, movies_batch, rows_buffer,
        genre_by_name, genre_by_tmdb,
        company_by_name, company_by_tmdb,
        country_by_code, lang_by_code
    ):
        """Insert/update Movies, then handle M2M relations."""

        # 1) Upsert Movies by tmdb_id
        created, updated = 0, 0
        existing = {m.tmdb_id: m for m in Movie.objects.filter(
            tmdb_id__in=[m.tmdb_id for m in movies_batch]
        )}

        # Create or update
        to_create = []
        to_update = []
        for m in movies_batch:
            if m.tmdb_id in existing:
                dbm = existing[m.tmdb_id]
                # Update fields (keep pk)
                dbm.imdb_id = m.imdb_id
                dbm.belongs_to_collection_raw = m.belongs_to_collection_raw
                dbm.budget = m.budget
                dbm.genres_raw = m.genres_raw
                dbm.homepage = m.homepage
                dbm.original_language_code = m.original_language_code
                dbm.original_title = m.original_title
                dbm.overview = m.overview
                dbm.popularity = m.popularity
                dbm.production_companies_raw = m.production_companies_raw
                dbm.production_countries_raw = m.production_countries_raw
                dbm.release_date = m.release_date
                dbm.revenue = m.revenue
                dbm.runtime = m.runtime
                dbm.spoken_languages_raw = m.spoken_languages_raw
                dbm.status = m.status
                dbm.tagline = m.tagline
                dbm.title = m.title
                dbm.vote_average = m.vote_average
                dbm.vote_count = m.vote_count
                to_update.append(dbm)
            else:
                to_create.append(m)

        if to_create:
            Movie.objects.bulk_create(to_create, ignore_conflicts=True)
            created += len(to_create)

        if to_update:
            Movie.objects.bulk_update(
                to_update,
                fields=[
                    "imdb_id", "belongs_to_collection_raw", "budget", "genres_raw",
                    "homepage", "original_language_code", "original_title", "overview",
                    "popularity", "production_companies_raw", "production_countries_raw",
                    "release_date", "revenue", "runtime", "spoken_languages_raw",
                    "status", "tagline", "title", "vote_average", "vote_count"
                ]
            )
            updated += len(to_update)

        # Refresh a map of tmdb_id -> Movie (now in DB) for M2M linking
        tmdb_to_movie = {
            m.tmdb_id: m
            for m in Movie.objects.filter(tmdb_id__in=[x.tmdb_id for x in movies_batch])
        }

        # 2) Resolve original_language FK by iso code
        for row in rows_buffer:
            code = (row.get("original_language") or "").strip()
            if not code:
                continue
            if code not in lang_by_code:
                lang_by_code[code], _ = Language.objects.get_or_create(
                    iso_639_1=code, defaults={"name": code}
                )
        # bulk assign original_language
        for row in rows_buffer:
            tmdb_id = safe_int(row.get("id"))
            movie = tmdb_to_movie.get(tmdb_id)
            if not movie:
                continue
            code = (row.get("original_language") or "").strip()
            movie.original_language = lang_by_code.get(code)
            movie.save(update_fields=["original_language"])

        # 3) Parse/attach Genres, Companies, Countries, Spoken Languages
        for row in rows_buffer:
            tmdb_id = safe_int(row.get("id"))
            movie = tmdb_to_movie.get(tmdb_id)
            if not movie:
                continue

            # --- Genres ---
            genres_list = []
            raw_genres = parse_json_array(row.get("genres"))
            # self.stdout.write(f"DEBUG genres raw: {raw_genres}")  # uncomment to debug
            for g in raw_genres:
                g_id = safe_int(g.get("id"))
                g_name = (g.get("name") or "").strip()
                if not g_name:
                    continue
                obj = None
                if g_id and g_id in genre_by_tmdb:
                    obj = genre_by_tmdb[g_id]
                elif g_name in genre_by_name:
                    obj = genre_by_name[g_name]
                    # fill tmdb_id if we learn it now
                    if g_id and obj.tmdb_id is None:
                        obj.tmdb_id = g_id
                        obj.save(update_fields=["tmdb_id"])
                        genre_by_tmdb[g_id] = obj
                else:
                    obj = Genre.objects.create(tmdb_id=g_id, name=g_name)
                    if g_id:
                        genre_by_tmdb[g_id] = obj
                    genre_by_name[g_name] = obj
                genres_list.append(obj)
            movie.genres.set(genres_list, clear=True)  # empty list is OK (clears)

            # --- Production companies ---
            companies_list = []
            raw_companies = parse_json_array(row.get("production_companies"))
            # self.stdout.write(f"DEBUG companies raw: {raw_companies}")  # uncomment to debug
            for c in raw_companies:
                c_id = safe_int(c.get("id"))
                c_name = (c.get("name") or "").strip()
                if not c_name:
                    continue
                obj = None
                if c_id and c_id in company_by_tmdb:
                    obj = company_by_tmdb[c_id]
                elif c_name in company_by_name:
                    obj = company_by_name[c_name]
                    if c_id and obj.tmdb_id is None:
                        obj.tmdb_id = c_id
                        obj.save(update_fields=["tmdb_id"])
                        company_by_tmdb[c_id] = obj
                else:
                    obj = Company.objects.create(tmdb_id=c_id, name=c_name)
                    if c_id:
                        company_by_tmdb[c_id] = obj
                    company_by_name[c_name] = obj
                companies_list.append(obj)
            movie.production_companies.set(companies_list, clear=True)

            # --- Production countries ---
            countries_list = []
            raw_countries = parse_json_array(row.get("production_countries"))
            # self.stdout.write(f"DEBUG countries raw: {raw_countries}")  # uncomment to debug
            for c in raw_countries:
                code = (c.get("iso_3166_1") or "").strip()
                cname = (c.get("name") or code or "").strip()
                if not code:
                    continue
                obj = country_by_code.get(code)
                if not obj:
                    # Use get_or_create so it’s safe on concurrent runs
                    obj, _ = Country.objects.get_or_create(
                        iso_3166_1=code,
                        defaults={"name": cname or code}
                    )
                    # Keep cache warm
                    country_by_code[code] = obj
                else:
                    # Upgrade name if it was just a placeholder
                    if obj.name == obj.iso_3166_1 and cname and cname != code:
                        obj.name = cname
                        obj.save(update_fields=["name"])
                countries_list.append(obj)
            movie.production_countries.set(countries_list, clear=True)

            # Spoken languages
            langs = []
            for l in parse_json_array(row.get("spoken_languages")):
                code = (l.get("iso_639_1") or "").strip()
                name = (l.get("name") or code or "").strip()
                if not code:
                    continue
                obj = lang_by_code.get(code)
                if not obj:
                    obj = Language.objects.create(iso_639_1=code, name=name)
                    lang_by_code[code] = obj
                elif obj.name == obj.iso_639_1 and name and name != code:
                    # Upgrade placeholder name to a proper one if available
                    obj.name = name
                    obj.save(update_fields=["name"])
                langs.append(obj)
            if langs:
                movie.spoken_languages.set(langs, clear=True)
            else:
                movie.spoken_languages.clear()

        return created, updated