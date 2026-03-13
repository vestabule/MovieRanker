from django.urls import path
from . import views


urlpatterns = [
    path("login/", view=views.user_login, name="login"),
    path("logout/", view=views.user_logout, name="logout"),
    path("user/", view=views.user_page, name="user"),
    path("user_signup/", view=views.user_signup, name="user_signup"),
    path("backend_movies/", view=views.movies, name="backend_movies"),
    path("backend_movie/", view=views.movie_details, name="backend_movie_details"),
    path("backend_ratings/", view=views.movie_ratings, name="backend_ratings")
]