"""
URL configuration for movierankings project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
#from movieranker.views import home, home_redirect, movies, movie_details, rate_movie
from movieranker import views
from frontend import views as fviews

urlpatterns = [
    path('admin/', admin.site.urls, name="admin_panel"),
    
    # # Built-in auth views: /accounts/login/, /logout/, /password_change/, etc.
    # path("accounts/", include("django.contrib.auth.urls")),

    # # Our custom signup + dashboard
    # path("accounts/", include("accounts.urls")),

    # Home page
    #path("", views.home_redirect, name="home_redirect"),
    #path("home/", views.home, name="home"),

    # Urls for movies
    path("", include("movieranker.urls")),
    path("", include("frontend.urls")),

    # Urls for movies - doing here to avoid having /movieranker/ in the url
    #path("movies/", views.movies, name="movies"),
    #path("movies/<int:pk>/", views.movie_details, name="movie"),
    #path("movie/<int:pk>/rate", views.rate_movie, name="rate"),
    #path("movies/ratings", views.user_ratings, name="user_ratings"),
    #path("movies/ratings/update/", views.update_rating, name="update_rating"),
    #path("movies/ratings/delete/", views.delete_rating, name="delete_rating"),
]
