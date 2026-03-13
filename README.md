
# Movie Ranker

A basic Django application to search and sort through movies, as well as allowing users to add rankings of their own.

## Outline and Architecture

The main purpose of this project is to provide a set of API's through which you can interact with a series of databases containing information on a variety of movies, though it does also support user account creation/login and deletion. 
The API's are designed to parse a json input and return a json output.

The full API reference can be found [here](./API_Reference.pdf)

When you are logged in, you are also able to leave your own rating for movies.

This is all contained in the Django app called movieranker.

To aid with visualisation, there is another app simply called frontend that provides a very basic frontend through which to interact with the API's. The frontend is simple HTML and JS, not requiring any additional dependencies.


## Setup up Python Virtual Environment and Install Django

Create a python virtual environemt and install Django

There are many ways to do this, here is an example using pip

If you already have Django installed, can skip this
    
    python -m venv <environment_name>
    source <environemtn_name>/bin/activate
    pip install django

## To Build Database


Download "The Movies Datasset" from Kaggle: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/

Extract and then run the following:

    python manage.py migrate
    python manage.py import_movies_metadata --path /path/to/movie_metadata.csv --batch-size 1000
    python manage.py import_links --path /path/to/links.csv --batch-size 10000 --backfill-imdb
    python manage.py import_ratings --path /path/to/ratings_small.csv --batch-size 5000 --resolve-movie
    python manage.py import_credits --path /path/to/credits.csv --batch-size 5000 --sqlite-fast
    python manage.py import_keywords --path /path/to/keywords.csv --batch-size 10000 --sqlite-fast


## Running the Application in a Dev Environmnet
(this application is not suitable for use production)

Run the following command from within the movierankings/ directory:

    python manage.py runserver
