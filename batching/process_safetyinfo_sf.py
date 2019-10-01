import os
import geohash
from util import create_spark_session
from util import write_to_postgres
from schema import INCIDENTS_SCHEMA_2003_SF, INCIDENTS_SCHEMA_SF, COMPLAIN_SCHEMA_SF


def _geo_encode(x, y, precision):
    try:
        return geohash.encode(float(x), float(y), precision)
    except:
        return ""


def process():
    """
    This function reads all the *.csv safety info files of San Francisco from S3 and clean them then save to Postgres.
    :type
    :rtype
    """
    source = 's3a://data-harbor/safetyinfo/sf/2003_to_May_2018.csv'
    source2 = 's3a://data-harbor/safetyinfo/sf/2018_to_Present.csv'
    source3 = 's3a://data-harbor/safetyinfo/sf/311_Cases.csv'
    spark = create_spark_session('process-sfsafety-data: San Francisco')

    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .schema(INCIDENTS_SCHEMA_SF) \
        .load(source)
    df.createOrReplaceTempView("incidents")
    """
    type 1 is Incidents, type 2 is 311-complains;
    """
    sql = """
        SELECT DISTINCT incident_number as ID, incident_date as occur_date, incident_description as description,
        'sf' as city, 1 as type, latitude, longitude
        FROM incidents 
        WHERE latitude is not null AND longitude is not null
        AND incident_date like '20__/__/__'
    """
    df_incident = spark.sql(sql)

    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .schema(INCIDENTS_SCHEMA_2003_SF) \
        .load(source2)
    df.createOrReplaceTempView("incidents")
    sql = """
        SELECT DISTINCT incident_number as ID, incident_date as occur_date, incident_description as description,
        'sf' as city, 1 as type, latitude, longitude
            FROM incidents 
            WHERE latitude is not null AND longitude is not null
            AND incident_date like '__/__/20__'
    """
    df_incident_2003 = spark.sql(sql)

    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .schema(COMPLAIN_SCHEMA_SF) \
        .load(source3)
    df.createOrReplaceTempView("incidents")
    sql = """
        SELECT DISTINCT caseid as ID, opened as occur_date, Request_type as description,
        'sf' as city, 2 as type, latitude, longitude
            FROM incidents 
            WHERE latitude is not null AND longitude is not null
            AND opened like '__/__/20__%'
    """
    df_complain = spark.sql(sql)

    df = df_incident.union(df_incident_2003).union(df_complain)
    df = df.withColumn('latitude', df.latitude.cast('FLOAT'))
    df = df.withColumn('longitude', df.longitude.cast('FLOAT'))
    df.createOrReplaceTempView("incidents")

    write_to_postgres(df, 'safety_info_san_francisco', 'append')

    spark.udf.register("geo_encode", lambda x, y, precision: _geo_encode(x, y, precision))
    spark.udf.register("geo_decode_x", lambda code: geohash.decode(code)[0])
    spark.udf.register("geo_decode_y", lambda code: geohash.decode(code)[1])

    """
    Javascript cannot take millions of records to develop a heatmap, so here use GeoHash to aggregate the results;
    """
    sql = """
        SELECT geo_decode_x(g.geo_code) as latitude, geo_decode_y(g.geo_code) as longitude, 
        count(*) as weight, 'sf' as city 
          FROM ( SELECT geo_encode(latitude, longitude, 7) as geo_code FROM incidents ) g 
          WHERE g.geo_code != ''
          GROUP BY g.geo_code
    """
    df_geo = spark.sql(sql)

    write_to_postgres(df_geo, 'heatmap_san_francisco', 'append')


if __name__ == '__main__':
    process()
