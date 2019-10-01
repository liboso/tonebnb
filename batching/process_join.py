import sys

from util import create_spark_session, write_to_postgres
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def process_computation(city):
    """
    This function reads all the listing, Listing_Detail, Reviews parquet format files of a specific city from S3
    Calculate the listing's score according to ranking, numver of reviews, and sentiment score of reviews,
    then save the results into Postgres.
    :type city: A specific U.S. or Canada city which has airbnb data in http://insideairbnb.com/
    :rtype    : Listing's basic info and a new attribute 'final_score'
    """
    source_listing = 's3a://data-harbor/airbnb/parquet/' + city + '/listings'
    source_listing_detail = 's3a://data-harbor/airbnb/parquet/' + city + '/detaillistings'
    source_review = 's3a://data-harbor/airbnb/parquet/' + city + '/reviews'

    # source_listing = '/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/listing'
    # source_listing_detail = '/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/detailedlisting'
    # source_review = '/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/review'

    spark = create_spark_session('process-computation:' + city)

    listing_df = spark.read.parquet(source_listing) \
        .select('id', 'name', 'host_name','neighbourhood', 'latitude', 'longitude',
                'room_type', 'price', 'minimum_nights', 'number_of_reviews')

    listing_detail_df = spark.read.parquet(source_listing_detail) \
        .select('id', 'review_scores_rating')

    review_df = spark.read.parquet(source_review) \
        .select('listing_id', 'comments')

    listing_df.createOrReplaceTempView("listings")
    listing_detail_df.createOrReplaceTempView("listing_detail")
    review_df.createOrReplaceTempView("reviews")

    neighbourhood_df = spark.sql('select neighbourhood, max(number_of_reviews) as max_number \
                                  FROM listings group by neighbourhood')

    listing_score_df = spark.sql('select listings.ID, neighbourhood, listings.number_of_reviews, \
                             0.05 * SUM(review_scores_rating)/COUNT(listings.ID) as avgRating \
                             FROM listings INNER JOIN listing_detail \
                             ON listings.ID = listing_detail.ID \
                             GROUP BY listings.ID, neighbourhood, listings.number_of_reviews')
    # print(listing_df.count())

    neighbourhood_df.createOrReplaceTempView("neighbourhood")
    listing_score_df.createOrReplaceTempView("listing_score")

    main_df = spark.sql('select  listing_score.*, \
                        (5.0 * listing_score.number_of_reviews / neighbourhood.max_number )as review_num_score \
                        FROM listing_score INNER JOIN neighbourhood \
                        ON listing_score.neighbourhood LIKE neighbourhood.neighbourhood')

    main_df.createOrReplaceTempView("listing_score")

    spark.udf.register("sentiment_score", lambda x: SentimentIntensityAnalyzer().polarity_scores(x)['compound'])
    sql = """
         SELECT listing_id, sentiment_score(comments) as score
             FROM reviews
             WHERE comments is not null
     """

    review_df = spark.sql(sql)
    review_df.createOrReplaceTempView("reviews")

    sql = """
        SELECT listing_id, SUM(score)/COUNT(listing_id) as avgTone FROM reviews
        GROUP BY listing_id
    """
    review_df = spark.sql(sql)
    review_df.createOrReplaceTempView("reviews")

    sql = """
        SELECT listing_score.*, reviews.avgTone 
        FROM listing_score INNER JOIN reviews
        ON listing_score.ID = reviews.listing_id
    """
    review_df = spark.sql(sql)
    review_df.createOrReplaceTempView("listing_score")

    main_df = spark.sql('select  listing_score.*, \
                        (avgRating + review_num_score) * (1 + avgTone) as final_score \
                        FROM listing_score')
    main_df.createOrReplaceTempView("listing_score")

    final_df = spark.sql('select  listings.*, listing_score.final_score \
                        FROM listings INNER JOIN listing_score \
                        ON listings.ID = listing_score.ID')

    write_to_postgres(final_df, 'LISTINGS_'+city, 'append')

    # print(final_df.schema)
    # main_df.show(20)


if __name__ == '__main__':
    """
    Expected arg is city name
    :param args:
    :return:
    """
    # A simple error handling solution to ensure expected arguments are provided.
    assert len(sys.arg) >= 2, "Mismatched arguments. Please ensure city name is provided."

    city = sys.arg[1]
    process_computation(city)
