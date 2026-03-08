
# Movie Ranker

A basic Django application to view movie rankings, as well as allowing users to add rankings of their own.

## To Build Database
Download "The Movies Datasset" from Kaggle: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset/

Extract and then run the following:

    python manage.py migrate
    python manage.py import_movies_metadata --path /path/to/movie_metadata.csv --batch-size 1000
    python manage.py import_links --path /path/to/links.csv --batch-size 10000 --backfill-imdb
    python manage.py import_ratings --path /path/to/ratings_small.csv --batch-size 5000 --resolve-movie
    python manage.py import_credits --path "/path/to/credits.csv" --batch-size 2000
    python manage.py import_credits --path "/path/to/credits.csv" --batch-size 5000 --sqlite-fast
    # quick test# quick test/path/to/keywords.csv" --limit 20000 --batch-size 10000 --sqlite-fast
    python manage.py import_keywords --path "/path/to/keywords.csv" --batch-size 10000 --sqlite-fast



## Running the Application in a Dev Environmnet

Run the following command from within the movierankings/ directory:

    python manage.py runserver
