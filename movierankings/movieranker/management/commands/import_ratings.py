import csv
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from movieranker.models import Rating, Link


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


def as_float(x):
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() in {"nan", "null"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def as_dt_from_epoch(x):
    i = as_int(x)
    if i is None:
        return None
    # Ratings file uses seconds since epoch
    return datetime.fromtimestamp(i, tz=timezone.utc)


class Command(BaseCommand):
    help = "Import ratings_small.csv (MovieLens). Optionally resolve to Movie via Link.movie."

    def add_arguments(self, parser):
        parser.add_argument("--path", required=True, help="Path to ratings_small.csv")
        parser.add_argument("--batch-size", type=int, default=5000, help="Rows per transaction batch")
        parser.add_argument("--resolve-movie", action="store_true",
                            help="If set, attaches Rating.movie via Link(movieId -> movie).")

    def handle(self, *args, **opts):
        path = Path(opts["path"])
        if not path.exists():
            raise CommandError(f"File not found: {path}")

        batch_size = int(opts["batch_size"])
        resolve_movie = bool(opts["resolve_movie"])

        # Build quick in-memory map from MovieLens movieId -> Movie.pk via Link
        ml_to_movie_pk = {}
        if resolve_movie:
            ml_to_movie_pk = dict(
                Link.objects.exclude(movie__isnull=True).values_list("movieId", "movie_id")
            )

        created, updated, skipped = 0, 0, 0
        to_create, buffer = [], []

        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            required = {"userId", "movieId", "rating", "timestamp"}
            if not required.issubset(reader.fieldnames or []):
                raise CommandError(f"Unexpected columns in {path.name}: {reader.fieldnames}")

            for row in reader:
                user_id = as_int(row.get("userId"))
                movie_id = as_int(row.get("movieId"))
                rating = as_float(row.get("rating"))
                dt = as_dt_from_epoch(row.get("timestamp"))

                if user_id is None or movie_id is None or rating is None or dt is None:
                    skipped += 1
                    continue

                r = Rating(
                    userId=user_id,
                    movieId=movie_id,
                    rating=rating,
                    timestamp=dt,
                )

                if resolve_movie:
                    r.movie_id = ml_to_movie_pk.get(movie_id)

                to_create.append(r)
                buffer.append((user_id, movie_id, dt))

                if len(to_create) >= batch_size:
                    c, u = self._flush(to_create, buffer)
                    created += c; updated += u
                    to_create.clear(); buffer.clear()

            if to_create:
                c, u = self._flush(to_create, buffer)
                created += c; updated += u

        self.stdout.write(self.style.SUCCESS(
            f"Done. Created: {created}, Updated: {updated}, Skipped: {skipped}"
        ))

    @transaction.atomic
    def _flush(self, batch, buffer):
        # Upsert by (userId, movieId, timestamp)
        existing = {
            (x.userId, x.movieId, x.timestamp): x
            for x in Rating.objects.filter(
                userId__in=[r.userId for r in batch],
                movieId__in=[r.movieId for r in batch],
            ).only("id", "userId", "movieId", "timestamp")
        }

        to_create, to_update = [], []
        for r in batch:
            key = (r.userId, r.movieId, r.timestamp)
            if key in existing:
                db = existing[key]
                # update fields that might differ
                db.rating = r.rating
                db.movie_id = r.movie_id
                to_update.append(db)
            else:
                to_create.append(r)

        created = 0
        updated = 0

        if to_create:
            Rating.objects.bulk_create(to_create, ignore_conflicts=True)
            created = len(to_create)

        if to_update:
            Rating.objects.bulk_update(to_update, fields=["rating", "movie"])
            updated = len(to_update)

        return created, updated