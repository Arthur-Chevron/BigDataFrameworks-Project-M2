from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
import pyspark.sql.functions as F

input_path = "s3://nahle-bucket-datalake/emr/input/movielens/"
output_path = "s3://s3-de1-chevron1/output/movielens/"


def main():
    spark = SparkSession.builder.appName('Movielens').getOrCreate()

    # show uniqly warning
    spark.sparkContext.setLogLevel("WARN")

    ##Extract
    print('\nExtract datasets movies and ratings ===> ===> In progress')
    movies = spark.read.csv(input_path + "movies.csv", header=True, inferSchema=True)
    ratings = spark.read.csv(input_path + "ratings.csv", header=True, inferSchema=True)
    print('Extract datasets movies and ratings ===> ===> Finished')

    # Explore the different dataset : Movie
    print('\n==========================\n Exploration Movies Dataset \n==========================')
    print("Count :", movies.count())
    print("Describe :", movies.describe())
    print("Show :", movies.show())

    # Explore the different dataset : Ratings
    print('\n==========================\n Exploration Ratings Dataset \n==========================')
    print("Count :", ratings.count())
    print("Describe :", ratings.describe())
    print("Show :", ratings.show())
    
    # get the date of release
    movies = movies.withColumn('date', regexp_extract(F.col('title'), '\((\d{4})\)', 1))
    
    # get the avg of ratings and the number of rating
    ratings_avg_number = ratings.groupBy('movieId').agg(F.avg('rating').alias('rating_avg'), F.count('*').alias('number_of_rating'))

    ##Transform
    print('Create the new dataset ===> ===> In progress')
    df = movies.join(ratings_avg_number, on='movieId')
    print('Create the new dataset ===> ===> Finished')

    ##Load
    print('Load to ' + output_path + '===> ===> In progress')
    df.select(['movieId', 'date', 'title', 'genres', 'rating_avg', 'number_of_rating']).write.mode("overwrite").csv(output_path, header = True)
    print('Load to ' + output_path + '===> ===> Finished')


    # Explore the different dataset : Ratings
    print('\n\n==========================\n Exploration Final Dataset \n==========================')
    print("Count :", df.count())
    print("Describe :", df.describe())
    print("Show :", df.show())

    print('\nETL process ===> ===> Finished\n')


if __name__ == '__main__':
    main()

