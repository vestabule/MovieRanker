from django.urls import path
from . import views


urlpatterns = [
    #path("", views.movie_list, name="movie_list"),
    #path("<int:movie_id>/", views.movie_id, name="movie"),
    #path("<movie_title>/", views.movie_title, name="moive_title"),
    path("login/", view=views.user_login, name="login"),
    path("logout/", view=views.user_logout, name="logout"),
    path("user/", view=views.user_page, name="user"),
    path("user_signup/", view=views.user_signup, name="user_signup"),

]