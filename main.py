from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import *
import pyspark.sql.types
from pyspark.sql.types import *

# Create a SparkSession
spark = SparkSession.builder.appName("popular_movies").getOrCreate()
# Defining the schema for each of the columns of the dataset
ratingsSchema = StructType([
    StructField('UserID', IntegerType(), True),
    StructField('MovieID', IntegerType(), True),
    StructField('Rating', StringType(), True),
    StructField('TimeStamp', IntegerType(), True)
])

movieSchema = StructType([
    StructField('MovieID', IntegerType(), True),
    StructField('Title', StringType(), True),
    StructField('Genres', StringType(), True)
])

# load up movie data as dataframe
ratings_df = spark.read.option("header", True).schema(ratingsSchema).csv("C:/Users/swoo3/Downloads/ratings.csv")
movies_df = spark.read.option("header", True).schema(movieSchema).csv("C:/Users/swoo3/Downloads/movies.csv")
# checks each column to see how many null values are there
print("Checking if there's null values in the movie dataset")
col_movies_null_df = movies_df \
    .select([count(when(col(c).isNull(), c)).alias(c) for c in movies_df.columns])
col_movies_null_df.show()
print("Checking if there's null values in the ratings dataset ")
col_ratings_null_df = ratings_df \
    .select([count(when(col(c).isNull(), c)).alias(c) for c in ratings_df.columns])
col_ratings_null_df.show()
#
movies_df.show(20, truncate=False)
ratings_df.show(20, truncate=False)
# Check how many different User_ID and Movie_ID are there
print("How many different User_ID are there")
count_UserID = ratings_df.select(countDistinct("UserID"))
count_UserID.show()
print("How many different Movies are there")
count_MovieID = movies_df.select(countDistinct("MovieID"))
count_MovieID.show()
print("Movie Genres")
diff_genre_count = movies_df.withColumn("Genres", explode(split("Genres", "[|]")))
diff_genre_count.show(20, truncate=False)
diff_genre_count.select(countDistinct("Genres")).show()

# # This find a specific row of the dataset
# print(movies_df.collect()[0])
# # This displays the top rows of the dataset
# print(movies_df.head(3))
# # This displays the bottom rows of the dataset
# print(movies_df.tail(3))

# print(movies_df.select([
#     'MovieID',
#     'Title',
#     'Genres']).collect()[5]
# )

# List the number of movies according to their genre
genre_count = movies_df.groupBy('Genres').count()
genre_count.sort(desc("count")).show(40, truncate=False)

# List the movies that doesn't have a genre listed
movies_df.filter(movies_df.Genres == "(no genres listed)").show(20, truncate=False)
# Count how many movies that are listed as no genre
movie_count = movies_df.filter(movies_df.Genres == "(no genres listed)").count()
print("The Count for No Genre Listed is ", movie_count)
# #
# # Join two dataframes movies_df and ratings_df
# # truncate displays the full content of the columns without truncation(resize the file to a specified sized
# movie_ratings_df = movies_df \
#     .join(ratings_df, movies_df.MovieID == ratings_df.MovieID) \
#     .drop(ratings_df.MovieID)
# movie_ratings_df.show(20, truncate=False)

# Most Popular Movie
print("Most Popular Movies")
most_popular_df = ratings_df \
    .groupBy("MovieID") \
    .agg(count("UserID")) \
    .withColumnRenamed("count(UserID)", "Viewer_Count") \
    .sort(desc("Viewer_Count"))
most_popular_df.show()

most_popular_movies_df = movies_df \
    .join(most_popular_df, movies_df.MovieID == most_popular_df.MovieID) \
    .drop(most_popular_df.MovieID) \
    .sort(desc("Viewer_Count"))
most_popular_movies_df.show(20, truncate=False)

# Top Rated Movie
print("Highest Rated Movies")
top_rated_movie_df = ratings_df \
    .groupBy("MovieID") \
    .agg(avg(col("Rating"))) \
    .withColumnRenamed("avg(Rating)", "Average_Viewer_Rating") \
    .sort(desc("Average_Viewer_Rating"))
top_rated_movie_df.show(20, truncate=False)

popular_rated_df = most_popular_movies_df \
    .join(top_rated_movie_df, ["MovieID"]) \
    .withColumn("Average_Viewer_Rating", func.round(top_rated_movie_df["Average_Viewer_Rating"], 2)) \
    .sort(desc("Average_Viewer_Rating"), desc("Viewer_Count"))
popular_rated_df.show(20, truncate=False)

# Top Rated Movies with more 1000 views
popular_rated_df.where("Viewer_Count > 1000").show(10, truncate=False)

# Average Viewer_Count, Lowest Viewer_Count, Highest Viewer Count
print("The average, minimum, and maximum viewer count of the movie dataset")
popular_rated_df.select([mean('Viewer_Count'), min('Viewer_Count'), max('Viewer_Count')]).show(1)

# Standard Deviation Of The Ratings of Each Movie
print("The standard deviation of each movie")
ratings_stddev = ratings_df \
    .groupBy("MovieID") \
    .agg(count("UserID").alias("Viewer_Count"),
         avg(col("Rating")).alias("Avg_Rating"),
         stddev(col("Rating")).alias("stddev_rating")
         ) \
    .withColumn("Avg_Rating", round(col("Avg_Rating"), 2)) \
    .withColumn("stddev_rating", round(col("stddev_rating"), 2))\
    .where("Viewer_Count > 1000")

ratings_stddev.show(10)

stddev_movie = movies_df \
    .join(ratings_stddev, movies_df.MovieID == ratings_stddev.MovieID) \
    .drop(ratings_stddev.MovieID) \
    .sort(desc("Avg_Rating")) \
    .where("Viewer_Count > 1000")
stddev_movie.show(10, truncate=False)
