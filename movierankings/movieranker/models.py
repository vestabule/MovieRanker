from django.db import models

# Create your models here.


# movies/models.py
from django.db import models

# ---------- Lookup tables ----------
class Genre(models.Model):
    # from JSON: [{"id": <int>, "name": "..."}]
    tmdb_id = models.IntegerField(null=True, blank=True, unique=True)
    name = models.CharField(max_length=100, unique=True)
    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name

class Company(models.Model):
    # from JSON: [{"id": <int>, "name": "..."}]
    tmdb_id = models.IntegerField(null=True, blank=True, unique=True)
    name = models.CharField(max_length=200, unique=True)
    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name

class Country(models.Model):
    # from JSON: [{"iso_3166_1": "US", "name": "United States of America"}, ...]
    iso_3166_1 = models.CharField(max_length=2, unique=True)
    name = models.CharField(max_length=100)
    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name

class Language(models.Model):
    # from JSON: [{"iso_639_1": "en", "name": "English"}, ...]
    iso_639_1 = models.CharField(max_length=2, unique=True)
    name = models.CharField(max_length=100)
    class Meta:
        ordering = ["name"]
    
    def __str__(self):
        return self.name

# ---------- Movie (ordered to mirror movies_metadata.csv minus removed fields) ----------
# Kaggle order for reference:
# adult (REMOVED), belongs_to_collection, budget, genres, homepage, id(tmdb), imdb_id,
# original_language, original_title, overview, popularity, poster_path (REMOVED),
# production_companies, production_countries, release_date, revenue, runtime,
# spoken_languages, status, tagline, title, video (REMOVED), vote_average, vote_count
class Movie(models.Model):
    # belongs_to_collection (stringified JSON) — keep raw for traceability
    belongs_to_collection_raw = models.TextField(blank=True)

    # budget
    budget = models.BigIntegerField(null=True, blank=True)

    # genres (stringified JSON) -> normalized M2M + raw copy
    genres = models.ManyToManyField(Genre, related_name="movies", blank=True)
    genres_raw = models.TextField(blank=True)

    # homepage
    homepage = models.URLField(max_length=500, blank=True)

    # id (TMDB)
    tmdb_id = models.IntegerField(unique=True, db_index=True)

    # imdb_id
    imdb_id = models.CharField(max_length=20, null=True, blank=True, unique=True)

    # original_language (FK + raw code)
    original_language = models.ForeignKey(
        Language, null=True, blank=True, on_delete=models.SET_NULL, related_name="original_language_movies"
    )
    original_language_code = models.CharField(max_length=10, blank=True)

    # original_title
    original_title = models.CharField(max_length=300, blank=True)

    # overview
    overview = models.TextField(blank=True)

    # popularity
    popularity = models.FloatField(null=True, blank=True)

    # production_companies (stringified JSON) -> normalized M2M + raw
    production_companies = models.ManyToManyField(Company, related_name="movies", blank=True)
    production_companies_raw = models.TextField(blank=True)

    # production_countries (stringified JSON) -> normalized M2M + raw
    production_countries = models.ManyToManyField(Country, related_name="movies", blank=True)
    production_countries_raw = models.TextField(blank=True)

    # release_date
    release_date = models.DateField(null=True, blank=True, db_index=True)

    # revenue
    revenue = models.BigIntegerField(null=True, blank=True)

    # runtime
    runtime = models.FloatField(null=True, blank=True)

    # spoken_languages (stringified JSON) -> normalized M2M + raw
    spoken_languages = models.ManyToManyField(Language, related_name="spoken_language_movies", blank=True)
    spoken_languages_raw = models.TextField(blank=True)

    # status
    status = models.CharField(max_length=50, blank=True)

    # tagline
    tagline = models.CharField(max_length=500, blank=True)

    # title
    title = models.CharField(max_length=300, db_index=True)

    # vote_average
    vote_average = models.FloatField(null=True, blank=True)

    # vote_count
    vote_count = models.IntegerField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["release_date", "vote_average"]),
            models.Index(fields=["title"]),
            models.Index(fields=["-popularity"]),
        ]
        ordering = ["-release_date", "-vote_average"]
        verbose_name = "movie"
        verbose_name_plural = "movies"


    def __str__(self):
        return self.title

