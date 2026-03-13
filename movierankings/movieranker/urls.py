from django.urls import path
from . import views


urlpatterns = [
    path("api/login/", view=views.user_login, name="login"),
    path("api/logout/", view=views.user_logout, name="logout"),
    path("api/user_signup/", view=views.user_signup, name="user_signup"),
    path("api/delete_account/", view=views.delete_account, name="delete_account"),
    path("api/movies/", view=views.movies, name="api/movies"),
    path("api/movie/", view=views.movie_details, name="api/movie_details"),
    path("api/ratings/", view=views.movie_ratings, name="api/ratings"),
]