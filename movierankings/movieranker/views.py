from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.http import HttpResponse, Http404
from django.core.paginator import Paginator
from .models import Movie, Genre, Rating

def home(request):
    return render(request, "home")

def home_redirect(request):
    return redirect("home/")

def movies(request):
    movies = Movie.objects.all()
    genres = Genre.objects.order_by("name")

    # --- Filtering ---
    genre_id = request.GET.get("genre_id") or ""
    if genre_id:
        # If genres is JSONField/list of dicts, consider __icontains or more robust search
        movies = movies.filter(genres__id=genre_id)
    # genre_ids = request.GET.getlist("genre_id")
    # if genre_ids:
    #     movies = movies.filter(genres__id__in=genre_ids).distinct()


    min_year = request.GET.get("min_year")
    max_year = request.GET.get("max_year")
    if min_year:
        movies = movies.filter(release_date__year__gte=min_year)
    if max_year:
        movies = movies.filter(release_date__year__lte=max_year)

    # --- Sorting ---
    sort = request.GET.get("sort", "title")
    sort_options = {
        "title": "title",
        "popularity": "-popularity",
        "release": "-release_date",
        "rating": "-vote_average",
    }
    movies = movies.order_by(sort_options.get(sort, "title"))

    # --- Pagination ---
    paginator = Paginator(movies, 20)
    page = request.GET.get("page")
    page_obj = paginator.get_page(page)

    # Build a querystring for pagination links that keeps filters/sort but drops 'page'
    qs = request.GET.copy()
    qs.pop("page", None)
    preserved_query = qs.urlencode()

    return render(request, "movies/movies.html", {
        "movies": page_obj,
        "sort": sort,
        "genres": genres,
        "genre_id": genre_id,
        "min_year": min_year,
        "max_year": max_year,
        "preserved_query": preserved_query,  # for pagination links
    })

def movie_details(request, pk):
    # Prefetch genres to avoid extra queries
    movie = get_object_or_404(
        Movie.objects.select_related().prefetch_related("genres"),
        pk=pk
    )
    
    latest_user_rating = None
    if request.user.is_authenticated:
        ml_movie_id = getattr(movie, "movieId", movie.id)
        latest_user_rating = (
            Rating.objects
            .filter(user=request.user, movieId=ml_movie_id)
            .order_by("-timestamp")
            .first()
        )

    # If the list page passed us a 'return' URL, use it for the back button
    return_url = request.GET.get("return")

    context = {
        "movie": movie,
        "return_url": return_url,
        "latest_user_rating": latest_user_rating,
    }

    return render(request, "movies/movie_details.html", context)

@login_required
def rate_movie(request, pk):
    """
    Show a simple decimal input (0-5) and on POST:
    - Append a new Rating row (preserve history) with current timestamp
    - Set both movie FK and legacy movieId
    """
    movie = get_object_or_404(Movie, pk=pk)

    # Determine the ML/movieId to store; adjust to your Movie model.
    # If your Movie model has a field named 'movieId' (from MovieLens), use that.
    # Otherwise, we’ll fall back to the primary key.
    ml_movie_id = getattr(movie, "movieId", movie.id)

    # Load the latest rating this user left for this MovieLens movieId
    latest_rating = None
    if request.user.is_authenticated:
        latest_rating = (
            Rating.objects
            .filter(user=request.user, movieId=ml_movie_id)
            .order_by("-timestamp")
            .first()
        )

    error = None

    if request.method == "POST":
        score_str = request.POST.get("score", "").strip()
        try:
            score = float(score_str)
        except ValueError:
            error = "Please enter a valid decimal number."
        else:
            if not (0.0 <= score <= 5.0):
                error = "Rating must be between 0 and 5."
        
        if error is None:
            # APPEND a new rating row to keep history (fits your unique constraint).
            Rating.objects.create(
                user=request.user,             # FK (nullable in your model, but we set it)
                userId=getattr(request.user, "id", None),  # keep legacy if you still backfill it; optional
                movieId=ml_movie_id,          # legacy MovieLens id (or fallback to pk)
                rating=score,
                timestamp=timezone.now(),
                movie=movie,                   # FK to your Movie
            )
            return redirect("movie", pk=pk)

    return render(request, "movies/rate_movie.html", {
        "movie": movie,
        "latest_rating": latest_rating,  # so we can prefill the input
        "error": error,
    })




