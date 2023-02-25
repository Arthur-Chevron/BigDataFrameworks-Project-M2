from pyspark.sql import SparkSession
import pyspark.sql.functions as F

output_path = "s3://s3-de1-chevron1/output/movielens/"

def getBestMoviePerYear(df):
    print(">>> Get Best movie per year")
    df_best_per_year = df.groupBy('date').agg(F.max('rating_avg').alias('max_rating_avg'))
    df_best_movies_per_year = df.join(df_best_per_year, on='date') \
                                    .filter(F.col('rating_avg') == F.col('max_rating_avg')) \
                                    .drop('max_rating_avg') \
                                    .orderBy("year") 
    
    return df_best_movies_per_year.show()


def main():
    spark = SparkSession.builder.appName('Movielens').getOrCreate()

    # show uniqly warning
    spark.sparkContext.setLogLevel("WARN")

    ##Extract
    print('\nExtract datasets movies and ratings ===> ===> In progress')
    df = spark.read.csv(output_path, header=True, inferSchema=True)
    print('Extract datasets movies and ratings ===> ===> Finished')

    print(df.count())

    print('\n\n==========================\n Analysis Data \n==========================')

    # create the sql view to permform queries
    df.createOrReplaceTempView("movielens_view")

    # get the best movies per year
    print("-----     get the best movie per year   -----")
    spark.sql("SELECT date, title, rating_avg FROM movielens_view \
              WHERE rating_avg = (SELECT MAX(rating_avg) FROM movielens_view t2 WHERE t2.date = movielens_view.date) \
              ORDER BY date ASC").show()
    
    # get the best movies per genre
    print("-----     get the best movie per genre   -----")
    spark.sql("SELECT genres, title, rating_avg FROM movielens_view \
              WHERE rating_avg = (SELECT MAX(rating_avg) FROM movielens_view t2 WHERE t2.genres = movielens_view.genres)").show()
    
    # get the best Action per year
    print("-----     get the Action per year   -----")
    spark.sql("SELECT date, title, rating_avg FROM movielens_view WHERE genres LIKE '%Action%' \
                AND rating_avg = (SELECT MAX(rating_avg) FROM movielens_view t2 WHERE t2.date = movielens_view.date \
                AND t2.genres LIKE '%Action%') ORDER BY date ASC").show()
    

    # get the best romance per year
    print("-----     get the best Action per year   -----")
    spark.sql("SELECT date, title, rating_avg FROM movielens_view WHERE genres LIKE '%Romance%' \
                AND rating_avg = (SELECT MAX(rating_avg) FROM movielens_view t2 WHERE t2.date = movielens_view.date \
                AND t2.genres LIKE '%Romance%') ORDER BY date ASC").show()


if __name__ == '__main__':
    main()

