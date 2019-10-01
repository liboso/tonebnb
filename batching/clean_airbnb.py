import sys
from util import create_spark_session


def clean_listings(city):
    """
    This function reads all the *.csv listing files of a specific city from S3 and save back to S3 in parquet format.
    :type city: A specific U.S. or Canada city which has airbnb data in http://insideairbnb.com/
    :rtype    : Distinct listing data info written to S3 in parquet format.
    """
    source = 's3a://data-harbor/airbnb/' + city + '/listing*.csv'
    target = 's3a://data-harbor/airbnb/parquet/' + city + '/listings'
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .options(multiLine='true') \
        .load(source)
    df = df.withColumn('id', df.id.cast('INT'))
    df = df.withColumn('latitude', df.latitude.cast('DOUBLE'))
    df = df.withColumn('longitude', df.longitude.cast('DOUBLE'))
    df = df.withColumn('price', df.price.cast('INT'))
    df = df.withColumn('minimum_nights', df.minimum_nights.cast('INT'))
    df = df.withColumn('number_of_reviews', df.number_of_reviews.cast('INT'))

    df.createOrReplaceTempView("listings")

    """
    for the specific listing, there are multiple records throughout different months, each one with the same basic info
    but different reviews number. so here just take the record with the max review number (the latest one)
    """
    sql = """
        SELECT DISTINCT id, name, host_name, neighbourhood, latitude, longitude, 
        room_type, price, minimum_nights, number_of_reviews
        FROM listings D
        WHERE number_of_reviews = (SELECT MAX(number_of_reviews) FROM listings WHERE id = D.id) 
        AND id is not null
    """

    # temp_df = spark.sql(sql)
    # print(temp_df.show(10))

    spark.sql(sql).coalesce(1).write.parquet(target)


def clean_listing_details(city):
    """
    This function reads all the listing detail files of a specific city from S3 and save back to S3 in parquet format.
    listing detail has a lot of detail info regarding every listing, we take 'listing_url' and 'review_scores_rating'
    :type city: A specific U.S. or Canada city which has airbnb data in http://insideairbnb.com/
    :rtype    : Distinct listing detail data info written to S3 in parquet format.
    """
    source = 's3a://data-harbor/airbnb/' + city + '/detaillisting*.csv'
    target = 's3a://data-harbor/airbnb/parquet/' + city + '/detaillistings'

    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .options(multiLine='true') \
        .load(source)

    df = df.withColumn('id', df.id.cast('INT'))
    df = df.withColumn('review_scores_rating', df.review_scores_rating.cast('INT'))

    df.createOrReplaceTempView("listings_detail")

    """
    According to airbnb's customer FAQ, review_scores_rating should be in 0~100 
    But the data has some records out of range, so here only take 0~100
    """
    sql = """
        SELECT  distinct id, listing_url, review_scores_rating
        FROM listings_detail
        WHERE  review_scores_rating<100 AND  review_scores_rating>0
    """
    spark.sql(sql).coalesce(1).write.parquet(target)


def clean_reviews(city):
    """
    This function reads all the listing reviews data of a specific city from S3 and save back to S3 in parquet format.
    :type city: A specific U.S. or Canada city which has airbnb data in http://insideairbnb.com/
    :rtype    : Distinct listing data info written to S3 in parquet format.
    """
    source = 's3a://data-harbor/airbnb/' + city + '/review*.csv'
    target = 's3a://data-harbor/airbnb/parquet/' + city + '/reviews'

    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .options(multiLine='true') \
        .load(source)

    df = df.withColumn('listing_id', df.listing_id.cast('INT'))

    df.createOrReplaceTempView("reviews")
    sql = """
        SELECT DISTINCT listing_id, date, comments
        FROM reviews
        WHERE id IS NOT NULL 
        AND comments IS NOT NULL
    """
    spark.sql(sql).coalesce(1).write.parquet(target)


if __name__ == '__main__':
    """
    Expected arg is city name
    :param args:
    :return:
    """
    # A simple error handling solution to ensure expected arguments are provided.
    assert len(sys.arg) >= 2, "Mismatched arguments. Please ensure city name is provided."

    city = sys.arg[1]
    spark = create_spark_session('clean_airbnb_data:' + city)

    clean_listings(spark, city)
    clean_listing_details(spark, city)
    clean_reviews(spark, city)
