import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from movieranker.models import Link, Movie


def as_int(val):
    """Convert CSV value to int or return None."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in {"nan", "null"}:
        return None
    try:
        return int(s)
    except Exception:
        try:
            # Some files store imdbId with leading zeros; still int-able
            return int(float(s))
        except Exception:
            return None


class Command(BaseCommand):
    help = "Import links.csv (or links_small.csv) and link rows to existing Movies by tmdbId/imdbId. Optionally backfill missing Movie.imdb_id."

    def add_arguments(self, parser):
        parser.add_argument("--path", required=True, help="Path to links.csv or links_small.csv")
        parser.add_argument("--batch-size", type=int, default=5000, help="Rows per transaction batch")
        parser.add_argument(
            "--backfill-imdb",
            action="store_true",
            help="If set, fill Movie.imdb_id when missing and imdbId is present."
        )

    def handle(self, *args, **opts):
        path = Path(opts["path"])
        if not path.exists():
            raise CommandError(f"File not found: {path}")

        batch_size = int(opts["batch_size"])
        backfill_imdb = bool(opts["backfill_imdb"])

        created, updated, resolved_by_tmdb, resolved_by_imdb, unresolved = 0, 0, 0, 0, 0

        to_create = []
        buffer = []

        # Prime small caches for faster lookups
        # Only cache id->pk maps to keep memory modest.
        tmdb_to_movie = {m.tmdb_id: m.pk for m in Movie.objects.exclude(tmdb_id__isnull=True).only("id", "tmdb_id")}
        imdb_to_movie = {  # Map "tt1234567" -> pk
            m.imdb_id: m.pk for m in Movie.objects.exclude(imdb_id__isnull=True).only("id", "imdb_id")
        }

        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not {"movieId", "imdbId", "tmdbId"}.issubset(reader.fieldnames or []):
                raise CommandError(f"Unexpected columns in {path.name}: {reader.fieldnames}")

            for row in reader:
                ml_id = as_int(row.get("movieId"))
                imdb_num = as_int(row.get("imdbId"))
                tmdb_num = as_int(row.get("tmdbId"))

                link = Link(movieId=ml_id, imdbId=imdb_num, tmdbId=tmdb_num)
                to_create.append(link)
                buffer.append((ml_id, imdb_num, tmdb_num))

                if len(to_create) >= batch_size:
                    c, u, r_tmdb, r_imdb, unr = self._flush(
                        to_create, buffer, tmdb_to_movie, imdb_to_movie, backfill_imdb
                    )
                    created += c; updated += u
                    resolved_by_tmdb += r_tmdb; resolved_by_imdb += r_imdb; unresolved += unr
                    to_create.clear(); buffer.clear()

            if to_create:
                c, u, r_tmdb, r_imdb, unr = self._flush(
                    to_create, buffer, tmdb_to_movie, imdb_to_movie, backfill_imdb
                )
                created += c; updated += u
                resolved_by_tmdb += r_tmdb; resolved_by_imdb += r_imdb; unresolved += unr

        self.stdout.write(self.style.SUCCESS(
            f"Done. Created: {created}, Updated: {updated}, "
            f"Linked via TMDb: {resolved_by_tmdb}, via IMDb: {resolved_by_imdb}, Unresolved: {unresolved}"
        ))

    @transaction.atomic
    def _flush(self, links_batch, buffer, tmdb_to_movie, imdb_to_movie, backfill_imdb):
        created, updated, resolved_by_tmdb, resolved_by_imdb, unresolved = 0, 0, 0, 0, 0

        # Upsert by (movieId) to keep idempotence
        existing = {
            l.movieId: l for l in Link.objects.filter(movieId__in=[x.movieId for x in links_batch])
        }

        to_create = []
        to_update = []

        for link in links_batch:
            if link.movieId in existing:
                db = existing[link.movieId]
                db.imdbId = link.imdbId
                db.tmdbId = link.tmdbId
                to_update.append(db)
            else:
                to_create.append(link)

        if to_create:
            Link.objects.bulk_create(to_create, ignore_conflicts=True)
            created += len(to_create)

        if to_update:
            Link.objects.bulk_update(to_update, fields=["imdbId", "tmdbId"])
            updated += len(to_update)

        # Refresh a map from movieId -> Link after upsert
        ml_to_link = {l.movieId: l for l in Link.objects.filter(movieId__in=[x.movieId for x in links_batch])}

        # Resolve FK to Movie (prefer tmdb, then imdb)
        for (ml_id, imdb_num, tmdb_num) in buffer:
            link = ml_to_link.get(ml_id)
            if not link:
                continue

            movie_pk = None
            if tmdb_num and tmdb_num in tmdb_to_movie:
                movie_pk = tmdb_to_movie[tmdb_num]
                resolved_by_tmdb += 1
            elif imdb_num:
                imdb_key = f"tt{imdb_num:07d}" if imdb_num and imdb_num > 0 else None
                if imdb_key and imdb_key in imdb_to_movie:
                    movie_pk = imdb_to_movie[imdb_key]
                    resolved_by_imdb += 1

            if movie_pk:
                if link.movie_id != movie_pk:
                    link.movie_id = movie_pk
                    link.save(update_fields=["movie"])
                # Optional backfill of Movie.imdb_id
                if backfill_imdb and imdb_num:
                    imdb_key = f"tt{imdb_num:07d}"
                    # Write to the Movie only if it is empty
                    Movie.objects.filter(pk=movie_pk, imdb_id__isnull=True).update(imdb_id=imdb_key)
                    # Keep cache up-to-date
                    imdb_to_movie.setdefault(imdb_key, movie_pk)
            else:
                unresolved += 1

        return created, updated, resolved_by_tmdb, resolved_by_imdb, unresolved