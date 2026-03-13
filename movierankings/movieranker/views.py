from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.utils.dateparse import parse_date, parse_datetime
from django.http import HttpResponse, Http404, JsonResponse
from django.core.paginator import Paginator
from .models import Movie, Genre, MovieCredit, CreditRole, Rating, Link
from django.db.models import F, Max, OuterRef, Subquery
from django.views.decorators.http import require_POST
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate, login, logout
import json
from django.contrib.auth import get_user_model
from django.db.models import Q
from django.db import IntegrityError
from django.utils import timezone
import math

User = get_user_model()

@csrf_exempt
def user_login(request):

    if request.method != "POST":
        return JsonResponse({"detail": "POST only"}, status=405)

    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"detail": "Invalid JSON"}, status=400)
    
    username = data.get("username")
    password = data.get("password")

    user = authenticate(request, username=username, password=password)
    # If the user wasn't authenticated
    if user is None:
        return JsonResponse({"detail": "Invalid Credentials"}, status=400)

    # Djano creates a session cookie for the user, and populates request.user 
    login(request, user)
    return JsonResponse({"detail": "Login Successful"}, status=200)

@csrf_exempt 
def user_logout(request):

    if request.method != "POST":
        return JsonResponse({"detail": "POST only"}, status=405)

    if request.method == "POST":
        logout(request)
        return JsonResponse({"detail": "Logged out"})
    
    return JsonResponse({"detail": "Logout failed"}, status=405)

def user_page(request):

    if not request.user.is_authenticated:
        jr = JsonResponse({"detail": "authentication required"})
        jr.status_code = 401
        return jr
    
    if request.method == "GET":
        jr = JsonResponse({"detail": "hello " + request.user.username})
        return jr
    
    jr = JsonResponse({"detail": "user page failed"})
    jr.status_code = 405
    return jr

@csrf_exempt
def user_signup(request):

    if request.method != "POST":
        return JsonResponse({"detail": "POST only"}, status=405)
    
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"detail": "Invalid JSON"}, status=400)
    
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return JsonResponse({"detail": "Missing username or password"}, status=400)

    if User.objects.filter(username=username).exists():
        return JsonResponse({"detail": "Username already in use"}, status=400)
    
    User.objects.create_user(username=username, password=password)

    return JsonResponse({"detail": "User created successful"}, status=201)

@csrf_exempt
def movies(request):

    if request.method != "POST":
        return JsonResponse({"detail": "GET only"}, status=405)

    qs = Movie.objects.all().distinct()

    # --- FILTER: title contains ---
    title = request.GET.get("title")
    if title:
        qs = qs.filter(title__icontains=title)

    # --- FILTER: genre name ---
    genre = request.GET.get("genre")
    if genre:
        qs = qs.filter(genres__name__icontains=genre)

    # --- FILTER: director name ---
    director = request.GET.get("director")
    if director:
        qs = qs.filter(
            credits__role=CreditRole.CREW,
            credits__job__iexact="Director",
            credits__person__name__icontains=director
        )

    # ------------------------------------------------------------------------
    # Actors filter — cast search
    # ------------------------------------------------------------------------
    actor = request.GET.get("actor")
    if actor:
        qs = qs.filter(
            credits__role=CreditRole.ACTOR,
            credits__person__name__icontains=actor
        )

    # ------------------------------------------------------------------------
    # Language (by name or ISO 639-1 code)
    # ------------------------------------------------------------------------
    language = request.GET.get("language")
    if language:
        # Match either the related Language name or the stored code field
        qs = qs.filter(
            Q(original_language__name__icontains=language) |
            Q(original_language_code__iexact=language)
        )

    # ------------------------------------------------------------------------
    # Production company (by company name)
    #   - ManyToMany: Movie.production_companies -> Company.name
    # ------------------------------------------------------------------------
    company = request.GET.get("company")
    if company:
        qs = qs.filter(production_companies__name__icontains=company)

    # ------------------------------------------------------------------------
    # Production country (by name or ISO 3166-1 code)
    #   - ManyToMany: Movie.production_countries -> Country.name / iso_3166_1
    # ------------------------------------------------------------------------
    country = request.GET.get("country")
    if country:
        qs = qs.filter(
            Q(production_countries__name__icontains=country) |
            Q(production_countries__iso_3166_1__iexact=country)
        )


    # ------------------------------------------------------------------------
    # Rating and popularity range filters
    # ------------------------------------------------------------------------
    min_rating = request.GET.get("min_rating")
    if min_rating:
        qs = qs.filter(vote_average__gte=float(min_rating))

    max_rating = request.GET.get("max_rating")
    if max_rating:
        qs = qs.filter(vote_average__lte=float(max_rating))

    min_pop = request.GET.get("min_popularity")
    if min_pop:
        qs = qs.filter(popularity__gte=float(min_pop))

    max_pop = request.GET.get("max_popularity")
    if max_pop:
        qs = qs.filter(popularity__lte=float(max_pop))

    # ------------------------------------------------------------------------
    # Release date range
    # ------------------------------------------------------------------------
    date_from = request.GET.get("released_after")
    if date_from:
        parsed = parse_date(date_from)
        if parsed:
            qs = qs.filter(release_date__gte=parsed)

    date_to = request.GET.get("released_before")
    if date_to:
        parsed = parse_date(date_to)
        if parsed:
            qs = qs.filter(release_date__lte=parsed)

    # ------------------------------------------------------------------------
    # Keyword filter
    # ------------------------------------------------------------------------
    keyword = request.GET.get("keyword")
    if keyword:
        qs = qs.filter(
            movie_keywords__keyword__name__icontains=keyword
        )

    #------------------------------------------------------------------------
    # Sorting 
    # ------------------------------------------------------------------------
    sort_field = request.GET.get("sort")
    sort_dir = request.GET.get("dir", "asc").lower()  # default ascending

    # Mapping user-friendly names → actual DB fields
    sort_map = {
        "title": "title",
        "rating": "vote_average",
        "popularity": "popularity",
        "release_date": "release_date",
    }

    if sort_field in sort_map:
        django_field = sort_map[sort_field]

        # Add "-" prefix for descending
        if sort_dir == "desc":
            django_field = "-" + django_field

        qs = qs.order_by(django_field)

    
    # ---------------------- Pagination ---------------------- #
    try:
        page = int(request.GET.get("page", 1))
    except ValueError:
        page = 1
    try:
        size = int(request.GET.get("size", 20))
    except ValueError:
        size = 20

    # Safety bounds
    if page < 1:
        page = 1
    if size < 1:
        size = 20
    if size > 100:
        size = 100

    total = qs.count() 
    num_pages = max(1, math.ceil(total / size))
    if page > num_pages:
        page = num_pages

    offset = (page - 1) * size
    page_qs = qs[offset: offset + size]

    
    # Helpful navigation links (optional but nice)
    base = request.build_absolute_uri(request.path)
    qp = request.GET.copy()
    qp["size"] = size  # normalize
    def page_url(p):
        qp["page"] = p
        return f"{base}?{qp.urlencode()}"

    next_url = page_url(page + 1) if page < num_pages else None
    prev_url = page_url(page - 1) if page > 1 else None

    # ------------------------------------------------------------------------
    # Build JSON response
    # ------------------------------------------------------------------------
    results = []
    for m in page_qs:
        results.append({
            "id": m.id,
            "tmdb_id": m.tmdb_id,
            "title": m.title,
            "genres": list(m.genres.values_list("name", flat=True)),
            "release_date": m.release_date.isoformat() if m.release_date else None,
            "vote_average": m.vote_average,
            "popularity": m.popularity,
        })

    
    return JsonResponse({
        "results": results,
        "pagination": {
            "total": total,
            "page": page,
            "size": size,
            "num_pages": num_pages,
            "next": next_url,
            "prev": prev_url
        }    
    })


def movie_details(request):

    if request.method != "GET":
        return JsonResponse({"details": "GET only"}, status=405)

    id = request.GET.get("id")
    if not id:
        return JsonResponse({"details": "Request must include a movie ID"}, status=400)

    try:
        movie = Movie.objects.get(id=id)
    except:
        return JsonResponse({"details": "Movie not found"}, status=404)
    
    # --- Genres ---
    genres = [
        g["name"] for g in movie.genres.values()
    ]

    # --- Production companies ---
    production_companies = [
        c.name for c in movie.production_companies.all()
    ]

    # --- Production countries ---
    production_countries = [
        {"name": c.name, "iso_3166_1": c.iso_3166_1}
        for c in movie.production_countries.all()
    ]

    # --- Spoken languages ---
    spoken_languages = [
        {"name": l.name, "iso_639_1": l.iso_639_1}
        for l in movie.spoken_languages.all()
    ]

    # --- Keywords ---
    keywords = [
        mk.keyword.name for mk in movie.movie_keywords.select_related("keyword")
    ]

    # --- Cast (Actors) ---
    cast = []
    for credit in movie.credits.filter(role=CreditRole.ACTOR).select_related("person"):
        cast.append({
            "name": credit.person.name,
            "character": credit.character,
        })

    # --- Crew (Directors, etc.) ---
    crew = []
    for credit in movie.credits.filter(role=CreditRole.CREW).select_related("person"):
        crew.append({
            "name": credit.person.name,
            "job": credit.job,
            "department": credit.department,
        })

    # --- Assemble full result ---
    data = {
        "id": movie.id,
        "tmdb_id": movie.tmdb_id,
        "imdb_id": movie.imdb_id,
        "title": movie.title,
        "original_title": movie.original_title,
        "overview": movie.overview,
        "status": movie.status,
        "tagline": movie.tagline,
        "homepage": movie.homepage,
        "budget": movie.budget,
        "revenue": movie.revenue,
        "runtime": movie.runtime,
        "popularity": movie.popularity,
        "vote_average": movie.vote_average,
        "vote_count": movie.vote_count,
        "release_date": (
            movie.release_date.isoformat()
            if movie.release_date else None
        ),

        "original_language": (
            movie.original_language.name if movie.original_language else None
        ),
        "original_language_code": movie.original_language_code,

        # m2m + extra sets
        "genres": genres,
        "production_companies": production_companies,
        "production_countries": production_countries,
        "spoken_languages": spoken_languages,
        "keywords": keywords,

        # credits
        "cast": cast,
        "crew": crew,

        # raw JSON fields (if you still want them)
        #"belongs_to_collection_raw": movie.belongs_to_collection_raw.name,
        #"genres_raw": movie.genres_raw,
        #"production_companies_raw": movie.production_companies_raw,
        #"production_countries_raw": movie.production_countries_raw,
        #"spoken_languages_raw": movie.spoken_languages_raw,
    }

    return JsonResponse(data)

@csrf_exempt
def movie_ratings(request):
    
    #--------- Auth guard ----------
    user = request.user
    if not user.is_authenticated:
        return JsonResponse({"detail": "Authentication required"}, status=401)

    # =========================== GET ==============================
    if request.method == "GET":

        qs = Rating.objects.filter(user=user).select_related("movie").distinct()

        # --------- Filters (useful subset only) ---------
        # By movie title (only ratings that have a linked Movie)
        title = request.GET.get("title")
        if title:
            qs = qs.filter(movie__isnull=False, movie__title__icontains=title)

        # By genre name (requires linked Movie)
        genre = request.GET.get("genre")
        if genre:
            qs = qs.filter(
                movie__isnull=False,
                movie__genres__name__icontains=genre
            )

        # Rating range
        min_rating = request.GET.get("min_rating")
        if min_rating:
            qs = qs.filter(rating__gte=float(min_rating))
        max_rating = request.GET.get("max_rating")
        if max_rating:
            qs = qs.filter(rating__lte=float(max_rating))

        # By your rating timestamp range (ISO 8601 or YYYY-MM-DD)
        date_after = request.GET.get("date_after")
        if date_after:
            dt = parse_datetime(date_after) or (
                parse_date(date_after).isoformat() + "T00:00:00" if parse_date(date_after) else None
            )
            if dt:
                qs = qs.filter(timestamp__gte=dt)

        date_before = request.GET.get("date_before")
        if date_before:
            dt = parse_datetime(date_before) or (
                parse_date(date_before).isoformat() + "T23:59:59" if parse_date(date_before) else None
            )
            if dt:
                qs = qs.filter(timestamp__lte=dt)

        # --------- Sorting ---------
        # Allowed: rating (your score), timestamp (when you rated), title (movie title)
        sort_field = request.GET.get("sort", "").lower()  # rating | timestamp | title
        sort_dir = request.GET.get("dir", "asc").lower()  # asc | desc

        sort_map = {
            "rating": "rating",
            "timestamp": "timestamp",
            "title": "movie__title",
        }
        if sort_field in sort_map:
            f = sort_map[sort_field]
            if sort_dir == "desc":
                f = "-" + f
            qs = qs.order_by(f)

        # --------- Pagination (page/size) ---------
        try:
            page = int(request.GET.get("page", 1))
        except ValueError:
            page = 1
        try:
            size = int(request.GET.get("size", 20))
        except ValueError:
            size = 20

        size = max(1, min(size, 100))
        total = qs.count()
        num_pages = max(1, math.ceil(total / size))
        page = max(1, min(page, num_pages))
        offset = (page - 1) * size
        page_qs = qs[offset: offset + size]

        # --------- Build response ---------
        results = []
        for r in page_qs:
            m = r.movie
            results.append({
                "id": r.id,
                "movieId": r.movieId,
                "rating": r.rating,
                "timestamp": r.timestamp.isoformat(),
                "movie": None if m is None else {
                    "id": m.id,
                    "title": m.title,
                    "release_date": m.release_date.isoformat() if m.release_date else None,
                    "genres": list(m.genres.values_list("name", flat=True)),
                    "popularity": m.popularity,
                    "vote_average": m.vote_average,
                }
            })

        base = request.build_absolute_uri(request.path)
        qp = request.GET.copy()
        qp["size"] = size
        def page_url(p):
            qp["page"] = p
            return f"{base}?{qp.urlencode()}"

        next_url = page_url(page + 1) if page < num_pages else None
        prev_url = page_url(page - 1) if page > 1 else None

        return JsonResponse({
            "results": results,
            "pagination": {
                "total": total,
                "page": page,
                "size": size,
                "num_pages": num_pages,
                "next": next_url,
                "prev": prev_url
            }
        })

    # =========================== POST =============================
    if request.method == "POST":
        # Expect JSON body
        try:
            data = json.loads(request.body or "{}")
        except json.JSONDecodeError:
            return JsonResponse({"detail": "Invalid JSON"}, status=400)

        # Required fields
        movieId = data.get("movieId")        # this is MovieLens movieId
        rating_value = data.get("rating")    # float score

        if movieId is None or rating_value is None:
            return JsonResponse({"detail": "movieId and rating are required"}, status=400)

        # Validate rating
        try:
            rating_float = float(rating_value)
        except (TypeError, ValueError):
            return JsonResponse({"detail": "rating must be a number"}, status=400)

        rating_float = round(rating_float, 1)
        if rating_float < 0 or rating_float > 5:
            return JsonResponse({"details": "rating must be from 0-5"})

        # Get current timestamp
        timestamp = timezone.now()

        # ----------------------------------------------
        # Resolve Movie via Link table
        # ----------------------------------------------
        from .models import Link
        try:
            link = Link.objects.get(movieId=movieId)
        except Link.DoesNotExist:
            return JsonResponse({
                "detail": f"No Link entry found for movieId={movieId}"
            }, status=400)

        movie_obj = link.movie  # may be None if mapping not resolved yet
        if movie_obj is None:
            return JsonResponse({
                "detail": f"Link entry exists for movieId={movieId}, but link.movie is not set"
            }, status=400)

        # ----------------------------------------------
        # Duplicate protection (one rating per user/movie)
        # ----------------------------------------------
        if Rating.objects.filter(user=user, movieId=movieId).exists():
            return JsonResponse(
                {"detail": "You have already rated this movie."},
                status=409
            )
        print("Timestamp  " + str(timestamp))
        # ----------------------------------------------
        # Create the rating record
        # ----------------------------------------------
        try:
            r = Rating.objects.create(
                user=user,
                userId=user.id,
                movieId=movieId,
                rating=float(rating_float),
                timestamp=timestamp,
                movie=movie_obj,    # Auto-linked via Link table
            )
        except IntegrityError:
            # In case of a race with uniqueness constraint
            return JsonResponse(
                {"detail": "You have already rated this movie."},
                status=409
            )

        return JsonResponse({
            "id": r.id,
            "movieId": r.movieId,
            "rating": r.rating,
            "timestamp": r.timestamp.isoformat(),
            "movie": {
                "id": movie_obj.id,
                "title": movie_obj.title,
                "release_date": movie_obj.release_date.isoformat()
                    if movie_obj.release_date else None,
            }
        }, status=201)

    # =========================== PUT ==============================
    if request.method == "PUT":
        # Expect JSON body
        try:
            data = json.loads(request.body or "{}")
        except json.JSONDecodeError:
            return JsonResponse({"detail": "Invalid JSON"}, status=400)
        
        # Requires an id, and a rating
        rating_id = data.get("rating_id")       
        rating_score = data.get("rating")

        if rating_id is None or rating_score is None:
            return JsonResponse({"detail": "rating_id and rating are required"}, status=400)

        
        # --- Load the rating and enforce ownership ---
        try:
            r = Rating.objects.select_related("movie").get(id=rating_id)
        except Rating.DoesNotExist:
            return JsonResponse({"detail": "Rating not found"}, status=404)

        if r.user_id != user.id:
            return JsonResponse({"detail": "Forbidden"}, status=403)
        
        # Validate rating
        try:
            rating_float = float(rating_score)
        except (TypeError, ValueError):
            return JsonResponse({"detail": "rating must be a number"}, status=400)

        rating_float = round(rating_float, 1)
        if rating_float < 0 or rating_float > 5:
            return JsonResponse({"details": "rating must be from 0-5"})
        
        r.rating = rating_float
        r.timestamp = timezone.now()

        try:
            r.save()
        except IntegrityError:
            return JsonResponse(
                {"detail": "Update conflicts with an existing rating timestamp."},
                status=409
            )

        return JsonResponse({
            "details": "Rating updated",
            "rating": {
                "id": r.id,
                "movieId": r.movieId,
                "rating": r.rating,
                "timestamp": r.timestamp.isoformat(),
                "movie": None if r.movie is None else {
                    "id": r.movie.id,
                    "title": r.movie.title,
                    "release_date": (
                        r.movie.release_date.isoformat()
                        if r.movie.release_date else None
                    )
                }
            }
        }, status=200)

    # =========================== DELETE ===========================
    if request.method == "DELETE":
        
        # Requires an id
        rating_id = request.GET.get("rating_id")       

        if rating_id is None:
            return JsonResponse({"detail": "rating_id is required"}, status=400)

        # --- Load the rating and enforce ownership ---
        try:
            r = Rating.objects.select_related("movie").get(id=rating_id)
        except Rating.DoesNotExist:
            return JsonResponse({"detail": "Rating not found"}, status=404)

        if r.user_id != user.id:
            return JsonResponse({"detail": "Forbidden"}, status=403)
        
        r.delete()
        return JsonResponse({"detail": "Deleted"}, status=200)

    
    # ======================= Other methods ========================
    return JsonResponse({"detail": "Method not allowed"}, status=405)




# def home(request):
#     return render(request, "home.html")

# def signup(request):
#     return render(request, "signup.html")

# def home_redirect(request):
#     return redirect("home/")

# def movies(request):
#     movies = Movie.objects.all()
#     genres = Genre.objects.order_by("name")

#     # --- Filtering ---
#     genre_id = request.GET.get("genre_id") or ""
#     if genre_id:
#         # If genres is JSONField/list of dicts, consider __icontains or more robust search
#         movies = movies.filter(genres__id=genre_id)
#     # genre_ids = request.GET.getlist("genre_id")
#     # if genre_ids:
#     #     movies = movies.filter(genres__id__in=genre_ids).distinct()


#     min_year = request.GET.get("min_year")
#     max_year = request.GET.get("max_year")
#     if min_year:
#         movies = movies.filter(release_date__year__gte=min_year)
#     if max_year:
#         movies = movies.filter(release_date__year__lte=max_year)

#     # --- Sorting ---
#     sort = request.GET.get("sort", "title")
#     sort_options = {
#         "title": "title",
#         "popularity": "-popularity",
#         "release": "-release_date",
#         "rating": "-vote_average",
#     }
#     movies = movies.order_by(sort_options.get(sort, "title"))

#     # --- Pagination ---
#     paginator = Paginator(movies, 20)
#     page = request.GET.get("page")
#     page_obj = paginator.get_page(page)

#     # Build a querystring for pagination links that keeps filters/sort but drops 'page'
#     qs = request.GET.copy()
#     qs.pop("page", None)
#     preserved_query = qs.urlencode()

#     return render(request, "movies/movies.html", {
#         "movies": page_obj,
#         "sort": sort,
#         "genres": genres,
#         "genre_id": genre_id,
#         "min_year": min_year,
#         "max_year": max_year,
#         "preserved_query": preserved_query,  # for pagination links
#     })

# def movie_details(request, pk):

#     # Prefetch genres to avoid extra queries
#     movie = get_object_or_404(
#         Movie.objects.select_related().prefetch_related("genres"),
#         pk=pk
#     )
    
#     latest_user_rating = None
#     if request.user.is_authenticated:
#         ml_movie_id = getattr(movie, "movieId", movie.id)
#         latest_user_rating = (
#             Rating.objects
#             .filter(user=request.user, movieId=ml_movie_id)
#             .order_by("-timestamp")
#             .first()
#         )

#     # If the list page passed us a 'return' URL, use it for the back button
#     return_url = request.GET.get("return")

#     context = {
#         "movie": movie,
#         "return_url": return_url,
#         "latest_user_rating": latest_user_rating,
#     }

#     return render(request, "movies/movie_details.html", context)

# @login_required
# def rate_movie(request, pk):
#     """
#     Show a simple decimal input (0-5) and on POST:
#     - Append a new Rating row (preserve history) with current timestamp
#     - Set both movie FK and legacy movieId
#     """
#     movie = get_object_or_404(Movie, pk=pk)

#     # Determine the ML/movieId to store; adjust to your Movie model.
#     # If your Movie model has a field named 'movieId' (from MovieLens), use that.
#     # Otherwise, we’ll fall back to the primary key.
#     ml_movie_id = getattr(movie, "movieId", movie.id)

#     # Load the latest rating this user left for this MovieLens movieId
#     latest_rating = None
#     if request.user.is_authenticated:
#         latest_rating = (
#             Rating.objects
#             .filter(user=request.user, movieId=ml_movie_id)
#             .order_by("-timestamp")
#             .first()
#         )

#     error = None

#     if request.method == "POST":
#         score_str = request.POST.get("score", "").strip()
#         try:
#             score = float(score_str)
#         except ValueError:
#             error = "Please enter a valid decimal number."
#         else:
#             if not (0.0 <= score <= 5.0):
#                 error = "Rating must be between 0 and 5."
        
#         if error is None:
#             # APPEND a new rating row to keep history (fits your unique constraint).
#             Rating.objects.create(
#                 user=request.user,             # FK (nullable in your model, but we set it)
#                 userId=getattr(request.user, "id", None),  # keep legacy if you still backfill it; optional
#                 movieId=ml_movie_id,          # legacy MovieLens id (or fallback to pk)
#                 rating=score,
#                 timestamp=timezone.now(),
#                 movie=movie,                   # FK to your Movie
#             )
#             return redirect("movie", pk=pk)

#     return render(request, "movies/rate_movie.html", {
#         "movie": movie,
#         "latest_rating": latest_rating,  # so we can prefill the input
#         "error": error,
#     })

# #@login_required
# def user_ratings(request):
#     """
#     A page showing the current user's ratings with sorting & filtering.
#     Default shows the latest rating per movie; toggle 'history=all' to view all entries.
#     """
#     # ----- Read query params -----
#     sort = request.GET.get("sort", "time_desc")           # time_desc|time_asc|rating_desc|rating_asc|title_asc|title_desc
#     history = request.GET.get("history", "latest")        # latest|all
#     title = request.GET.get("title", "").strip()          # movie title contains
#     min_rating = request.GET.get("min_rating")            # decimal
#     max_rating = request.GET.get("max_rating")            # decimal
#     start_date = request.GET.get("start_date")            # YYYY-MM-DD
#     end_date = request.GET.get("end_date")                # YYYY-MM-DD
#     genre_ids = request.GET.getlist("genre_id")           # multi-select

#     # ----- Base queryset: this user only -----
#     qs = Rating.objects.filter(user=request.user)

#     # ----- Latest-only or full history -----
#     if history != "all":
#         # Keep only the latest rating per movieId using a Subquery (DB-agnostic)
#         latest_ts_subq = (
#             Rating.objects
#             .filter(user=request.user, movieId=OuterRef("movieId"))
#             .values("movieId")
#             .annotate(max_ts=Max("timestamp"))
#             .values("max_ts")[:1]
#         )
#         qs = qs.annotate(latest_ts=Subquery(latest_ts_subq)).filter(timestamp=F("latest_ts"))

#     # ----- Join to Movie for title/genre filters (skip when not needed) -----
#     # We’ll always prefetch movie/genres for rendering
#     qs = qs.select_related("movie").prefetch_related("movie__genres")

#     # ----- Filters -----
#     if title:
#         qs = qs.filter(movie__title__icontains=title, movie__isnull=False)

#     # Rating range
#     if min_rating:
#         try:
#             qs = qs.filter(rating__gte=float(min_rating))
#         except ValueError:
#             pass
#     if max_rating:
#         try:
#             qs = qs.filter(rating__lte=float(max_rating))
#         except ValueError:
#             pass

#     # Date range (expecting YYYY-MM-DD)
#     if start_date:
#         qs = qs.filter(timestamp__date__gte=start_date)
#     if end_date:
#         qs = qs.filter(timestamp__date__lte=end_date)

#     # Genre filter (multi)
#     if genre_ids:
#         qs = qs.filter(movie__isnull=False, movie__genres__id__in=genre_ids).distinct()

#     # ----- Sorting -----
#     sort_map = {
#         "time_desc": "-timestamp",
#         "time_asc": "timestamp",
#         "rating_desc": "-rating",
#         "rating_asc": "rating",
#         "title_asc": "movie__title",
#         "title_desc": "-movie__title",
#     }
#     qs = qs.order_by(sort_map.get(sort, "-timestamp"))

#     # ----- Pagination -----
#     paginator = Paginator(qs, 20)
#     page_number = request.GET.get("page")
#     page_obj = paginator.get_page(page_number)

#     # ----- For the genre filter dropdown -----
#     genres = Genre.objects.order_by("name")

#     # Preserve GET (except page) in pagination links and for inline form return
#     preserved = request.GET.copy()
#     preserved.pop("page", None)
#     preserved_query = preserved.urlencode()

#     context = {
#         "ratings": page_obj,
#         "genres": genres,
#         "selected_genres": genre_ids,
#         "sort": sort,
#         "history": history,
#         "title": title,
#         "min_rating": min_rating,
#         "max_rating": max_rating,
#         "start_date": start_date,
#         "end_date": end_date,
#         "preserved_query": preserved_query,
#     }

#     return render(request, "movies/user_ratings.html", context)


# @login_required
# @require_POST
# def update_rating(request):
#     movie_id = request.POST.get("movie_id")
#     score_str = request.POST.get("score", "").strip()
#     return_url = request.POST.get("return")  # full URL with query, from the page

#     # Validate
#     movie = get_object_or_404(Movie, pk=movie_id)
#     try:
#         score = float(score_str)
#     except ValueError:
#         # Fallback safe redirect
#         return redirect(return_url or "user_ratings")

#     if not (0.0 <= score <= 5.0):
#         return redirect(return_url or "user_ratings")

#     # Determine MovieLens ID or fallback to pk
#     ml_movie_id = getattr(movie, "movieId", movie.id)

#     # Append a new rating row (history preserved)
#     Rating.objects.create(
#         user=request.user,
#         userId=getattr(request.user, "id", None),  # legacy integer if you still backfill
#         movieId=ml_movie_id,
#         rating=score,
#         timestamp=timezone.now(),
#         movie=movie,
#     )
#     return redirect(return_url or "user_ratings")


# # TODO: with separate frontend swtich to Delete
# @login_required
# @require_POST
# def delete_rating(request):
#     """
#     Deletes a single Rating row (only if it belongs to the current user).
#     Redirects back to the supplied 'return' URL or the user_ratings page.
#     """
#     rating_id = request.POST.get("rating_id")
#     return_url = request.POST.get("return")

#     rating = get_object_or_404(Rating, id=rating_id, user=request.user)
#     rating.delete()
#     messages.success(request, "Rating deleted.")

#     # Return to the same list view (preserving filters/sort/page if provided)
#     if return_url:
#         return redirect(return_url)
#     return redirect("user_ratings")
    
