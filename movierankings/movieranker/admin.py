from django.contrib import admin

from .models import Movie, Genre, Company, Country, Language

# Register your models here.

# admin.site.register(Movie)
# admin.site.register(Genre)
# admin.site.register(Company)
# admin.site.register(Country)
# admin.site.register(Language)

# movies/admin.py
from django.contrib import admin
from .models import Movie, Genre, Company, Country, Language

@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):
    list_display = ("title", "tmdb_id", "release_date", "vote_average")
    search_fields = ("title", "original_title", "imdb_id", "tmdb_id")
    list_filter = (
        "release_date",
        ("original_language", admin.RelatedOnlyFieldListFilter),
        ("genres", admin.RelatedOnlyFieldListFilter),
        ("production_companies", admin.RelatedOnlyFieldListFilter),
        ("production_countries", admin.RelatedOnlyFieldListFilter),
    )
    # Makes the dual-selector UI very explicit for M2M:
    filter_horizontal = ("genres", "production_companies", "production_countries", "spoken_languages")

# Helpful search in related pickers
@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ("name",)

@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    search_fields = ("name",)

@admin.register(Country)
class CountryAdmin(admin.ModelAdmin):
    search_fields = ("name", "iso_3166_1")

@admin.register(Language)
class LanguageAdmin(admin.ModelAdmin):
    search_fields = ("name", "iso_639_1")