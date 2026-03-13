from django.urls import path
from . import views

urlpatterns = [
    path("home/", views.home, name="home"),
    path("demo/", views.demo, name="demo"),
    path("signup/", views.signup, name="signup"),
    path("movies/", views.movies, name="movies"),
    path("movie/", views.movie_details, name="movie"),
    path("ratings/", views.ratings, name="ratings")
]