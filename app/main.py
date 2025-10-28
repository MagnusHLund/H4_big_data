from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timezone

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Movies Data Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Load data from CSV files
data_directory = './data/ml-latest/'

movies = spark.read.csv(data_directory + 'movies.csv', sep=',',
                        inferSchema=True, header=True)

ratings = spark.read.csv(data_directory + 'ratings.csv', sep=',',
                         inferSchema=True, header=True)

# This function lists unique movie titles
def unique_movies(amount):
    print("Unique movies:")
    unique_movies = movies.select('title').distinct()
    unique_movies.show(amount)

# This function calculates the average movie rating
def average_movie_rating():
    print("Average movie rating:")
    avg_ratings = ratings.agg({'rating': 'avg'}).collect()[0][0]
    print(f"Average rating across all movies: {avg_ratings}")

# This function lists the highest rated movies
def highest_rated_movies(amount):
    print(f"Top {amount} highest rated movies:")
    avg_ratings = ratings.groupBy('movieId').avg('rating')
    top_movies = avg_ratings.orderBy('avg(rating)', ascending=False).limit(amount)
    top_movies.show()

# This function retrieves the latest rating entry
def latest_rating():
    print("Latest rating entry:")
    latest_entry = ratings.orderBy('timestamp', ascending=False).first()
    timestamp = datetime.fromtimestamp(latest_entry['timestamp'], timezone.utc)
    print(f"Latest rating: Movie ID {latest_entry['movieId']}, Rating: {latest_entry['rating']}, Timestamp: {timestamp}")

# This function gets average rating for a specific movie per year
def get_average_rating_for_movie_per_year(movie_id):
    print(f"Average rating for movie ID {movie_id} per year:")
    filtered_ratings = ratings.filter(ratings.movieId == movie_id)
    unix_timestamp = filtered_ratings.select('timestamp')
    ratings_with_year = filtered_ratings.withColumn('year', (unix_timestamp['timestamp'] / 31536000 + 1970).cast('integer'))
    avg_ratings_per_year = ratings_with_year.groupBy('year').avg('rating').orderBy('year', ascending=False)
    avg_ratings_per_year.show(10)

# This function matches movies with a specific genre
def match_movies_with_genre(amount, genre):
    print(f"Movies matching genre '{genre}':")
    matched_movies = movies.filter(movies.genres.contains(genre)).limit(amount)
    matched_movies.show()

# This function calculates average rating for each genre
def get_average_rating_for_genre():
    print("Average rating for each genre:")
    ratings_with_genres = ratings.join(movies, on='movieId', how='inner')
    genres_exploded = ratings_with_genres.withColumn('genre', F.explode(F.split(ratings_with_genres.genres, '\\|')))
    avg_ratings_by_genre = genres_exploded.groupBy('genre').avg('rating').orderBy('avg(rating)', ascending=False)
    avg_ratings_by_genre.show()

# This function finds the highest rated genre
def get_highest_rated_genre():
    print("Highest rated genre:")
    ratings_with_genres = ratings.join(movies, on='movieId', how='inner')
    genres_exploded = ratings_with_genres.withColumn('genre', F.explode(F.split(ratings_with_genres.genres, '\\|')))
    avg_ratings_by_genre = genres_exploded.groupBy('genre').avg('rating')
    highest_rated_genre = avg_ratings_by_genre.orderBy('avg(rating)', ascending=False).first()
    print(f"Highest rated genre: {highest_rated_genre['genre']} with average rating {highest_rated_genre['avg(rating)']}")  

# This function graphs average rating for each genre
def graph_average_rating_for_genres():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # Compute average rating per genre
    ratings_with_genres = ratings.join(movies, on='movieId', how='inner')
    genres_exploded = ratings_with_genres.withColumn('genre', F.explode(F.split(ratings_with_genres.genres, '\\|')))
    avg_ratings_by_genre = genres_exploded.groupBy('genre').avg('rating').orderBy('avg(rating)', ascending=False)
    avg_ratings_collected = avg_ratings_by_genre.collect()

    # Prepare data for plotting
    genres = [row['genre'] for row in avg_ratings_collected]
    avg_ratings = [row['avg(rating)'] for row in avg_ratings_collected]

    # Generates the chart
    plt.figure(figsize=(12, max(6, 0.25 * len(genres))))
    y_pos = list(range(len(genres)))
    plt.barh(y_pos, avg_ratings, color="lightgreen")
    plt.yticks(y_pos, genres)
    plt.xlabel("Average Rating")
    plt.title("Average Movie Ratings by Genre")
    plt.gca().invert_yaxis()
    plt.tight_layout()

    # Save the chart as a PNG file
    out_dir = "output"
    import os
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "genre_avg_ratings.png")
    plt.savefig(out_path)
    print(f"Saved chart to {out_path}")

unique_movies(10)
average_movie_rating()
highest_rated_movies(10)
latest_rating()
get_average_rating_for_movie_per_year(64)
match_movies_with_genre(10, "Comedy")
get_average_rating_for_genre()
get_highest_rated_genre()
graph_average_rating_for_genres()