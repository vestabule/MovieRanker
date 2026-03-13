from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.http import HttpResponse, Http404, JsonResponse
from django.core.paginator import Paginator
from .models import Movie, Genre, Rating
from django.db.models import F, Max, OuterRef, Subquery
from django.views.decorators.http import require_POST
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate, login, logout
import json
from django.contrib.auth import get_user_model

User = get_user_model()

@csrf_exempt
def user_login(request):

    if request.method == "POST":
        data = json.loads(request.body)
        username = data.get("username")
        password = data.get("password")

        user = authenticate(request, username=username, password=password)
        # If the user wasn't authenticated
        if user is None:
            jr = JsonResponse({"detail": "Invalid Credentials"})
            jr.status_code = 400
            return jr

        # Djano creates a session cookie for the user, and populates request.user 
        login(request, user)
        return JsonResponse({"detail": "Login Successful"})
    
    jr = JsonResponse({"detail": "Log in failed"})
    jr.status_code = 405
    return jr

@csrf_exempt 
def user_logout(request):

    if request.method == "POST":
        logout(request)
        return JsonResponse({"detail": "Logged out"})
    
    jr = JsonResponse({"detail": "Log out failed"})
    jr.status_code = 405
    return jr

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


# def home(request):
#     return render(request, "home.html")

# def signup(request):
#     return render(request, "signup.html")

# def home_redirect(request):
#     return redirect("home/")

# @csrf_exempt
# def movies(request):
#     #logout(request)
#     #return JsonResponse({"details": "logged out"})
#     u = request.user
#     return JsonResponse({"id": u.id, "username": u.username})
#     username = request.GET.get("username")
#     password = request.GET.get("password")
#     user = authenticate(request, username=username, password=password)
#     if not user:
#         return JsonResponse({"detail": "Invalid credentials"}, status=400)
#     login(request, user)
#     return JsonResponse({"detail": "Logged in"})


#     p = request.GET
#     # b = ""
#     # for k in p.all():
#     #     b += (k + "  ")
#     response = "This is the response: " + p.get("name", "backup")
#     # for x in request.body:
#     #     response += str(x) + " a "
#     data = {"tghin1": 123, "thing 2": {"sub thing": 10, "balh": "bluhh"}}
#     r = JsonResponse(data)
#     #r#.content = response
#     #r.status_code = 607
#     return r# HttpResponse(response)

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
    
