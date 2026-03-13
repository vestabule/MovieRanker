from django.utils.dateparse import parse_date, parse_datetime
from django.utils import timezone
from django.http import JsonResponse
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate, login, logout, get_user_model

from .models import Movie, CreditRole, Rating

import json
import math

User = get_user_model()


def resolve_movie(id: int | None = None, name: str | None = None, year: int | None = None):
    """
    Resolve a Movie by internal PK (id) or by title (optionally year).
    Returns (movie, error_json_response|None).

    Rules:
    - If id is provided -> fetch by PK (fast path).
    - Else if name given -> try iexact, fallback icontains; if not unique and year provided, use it.
    - Returns a JsonResponse error (404/409/400) if not resolvable, otherwise (movie, None).
    """
    if id is not None:
        try: id = int(id)
        except: return None, JsonResponse({"detail": f"Movie id must be an integer"}, status=400)

        try:
            m = Movie.objects.get(pk=id)
            return m, None
        except Movie.DoesNotExist:
            return None, JsonResponse({"detail": f"Movie id {id} not found"}, status=404)

    if name:
        qs = Movie.objects.filter(title__iexact=name)
        if not qs.exists():
            qs = Movie.objects.filter(title__icontains=name)

        if year is not None:
            qs = qs.filter(release_date__year=year)

        count = qs.count()
        if count == 0:
            return None, JsonResponse({"detail": f"No movie found matching '{name}'" + (f" ({year})" if year else "")}, status=404)
        if count > 1:
            # Provide suggestions to help client disambiguate
            suggestions = list(qs.values("id", "title", "release_date")[:10])
            return None, JsonResponse({
                "detail": "Movie name is not unique. Provide id or include a year.",
                "candidates": suggestions
            }, status=409)

        return qs.first(), None

    return None, JsonResponse({"detail": "Provide either 'id' or 'name' parameter"}, status=400)

@csrf_exempt
def user_login(request):

    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)

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

    # Django creates a session cookie for the user, and populates request.user 
    login(request, user)
    return JsonResponse({"message": "Login Successful"}, status=200)

@csrf_exempt 
def user_logout(request):

    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)

    if request.method == "POST":
        logout(request)
        return JsonResponse({"message": "Logged out"})
    
    return JsonResponse({"detail": "Logout failed"}, status=405)

@csrf_exempt
def delete_account(request):

    user = request.user
    if not user.is_authenticated:
        return JsonResponse({"detail": "Authentication required"}, status=401)

    if request.method not in ("DELETE", "POST"):
        return JsonResponse({"detail": "Method not allowed"}, status=405)

    # Look for parameters in json body and http parameters
    try:
        body = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        body = {}

    confirm = request.GET.get("confirm") or body.get("confirm")
    password = request.GET.get("password") or body.get("password")
    
    if not confirm or confirm != "DELETE":
        return JsonResponse({"detail": "Confirmation required: confirm='DELETE'."}, status=400)

    if not user.check_password(password):
        return JsonResponse({"detail": "Password is either missing or invalid"}, status=403)


    # End the user's session
    logout(request)

    # Perform the delete in a transaction
    with transaction.atomic():
        user.delete()

    return JsonResponse({"message": "Deleted"}, status=200)

@csrf_exempt
def user_signup(request):

    if request.method != "POST":
        return JsonResponse({"detail": "Method not allowed"}, status=405)
    
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

    return JsonResponse({"message": "User created successful"}, status=201)

@csrf_exempt
def movies(request):

    if request.method != "GET":
        return JsonResponse({"detail": "Method not allowed"}, status=405)

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
            Q(original_language__name__icontains=language) | Q(original_language_code__iexact=language)
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
            Q(production_countries__name__icontains=country) | Q(production_countries__iso_3166_1__iexact=country)
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
        return JsonResponse({"detail": "Method not allowed"}, status=405)

    id = request.GET.get("id")
    name = request.GET.get("name")
    year = request.GET.get("year")
    try: year = int(year)
    except: year = None

    movie, err = resolve_movie(id, name, year)
    if err:
        return err

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

        # Fields
        movieId = data.get("movieId")        
        rating_value = data.get("rating") 

        if movieId is None or rating_value is None:
            return JsonResponse({"detail": "movieId and rating are required"}, status=400)

        # Validate rating
        try:
            rating_float = float(rating_value)
        except (TypeError, ValueError):
            return JsonResponse({"detail": "rating must be a number"}, status=400)

        rating_float = round(rating_float, 1)
        if rating_float < 0 or rating_float > 5:
            return JsonResponse({"detail": "rating must be from 0-5"})

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
            return JsonResponse({"detail": "rating must be from 0-5"})
        
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

        # Look for rating id in body or parameters
        try:
            body = json.loads(request.body or "{}")
        except json.JSONDecodeError:
            body = {}

        rating_id = request.GET.get("rating_id") or body.get("rating_id")
        
        if not rating_id:
            return JsonResponse({"detail": "rating_id is required"}, status=400)
     
        # --- Load the rating and enforce ownership ---
        try:
            r = Rating.objects.select_related("movie").get(id=rating_id)
        except Rating.DoesNotExist:
            return JsonResponse({"detail": "Rating not found"}, status=404)

        if r.user_id != user.id:
            return JsonResponse({"detail": "Forbidden"}, status=403)
        
        r.delete()
        return JsonResponse({"message": "Deleted"}, status=200)

    
    # ======================= Other methods ========================
    return JsonResponse({"detail": "Method not allowed"}, status=405)
