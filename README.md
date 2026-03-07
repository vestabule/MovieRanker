
# Movie Ranker

A basic Django application to view movie rankings, as well as allowing users to add rankings of their own.

## To Build Database
Download "The Movies Datasset" from Kaggle: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/

Extract and then run the following:

    python manage.py migrate
    python manage.py import_movies_metadata --path /path/to/movie_metadata.csv

## Running the Application in a Dev Environmnet

Run the following command from within the movierankings/ directory:

    python manage.py runserver
