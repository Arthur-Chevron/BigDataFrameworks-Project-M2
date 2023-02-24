from pyspark.sql import SparkSession

import pyspark.sql.functions as F


input_path = "s3://nahle-bucket-datalake/emr/input/movielens/"
output_path = "s3://nahle-bucket-datalake/emr/output/movielens/"
def main():
    
    ##Extract
    spark = SparkSession.builder.appName('NahleSparkSurvery').getOrCreate()
    movies  = spark.read.csv(input_path+"/movies.csv", header = True, inferSchema = True)
    ratings  = spark.read.csv(input_path+"/ratings.csv", header = True, inferSchema = True)
    ##Transform
    ratings_date = ratings.withColumn("date", F.from_unixtime(ratings.timestamp))  
    joinedDF = ratings_date.join(movies, ratings_date.movieId==movies.movieId, "inner")
    ##Load
    joinedDF.select(['userId', 'rating', 'date', 'title', 'genres']).write.csv(output_path, header = True)
    joinedDF.show(10)
    print("-----------ETL process finished--------------")
    print("-----------ETL process finished--------------")

if __name__ == '__main__':
    main()

