import os
import json
import boto3
from botocore.exceptions import ClientError

from util import create_spark_session
from util import write_to_postgres
from pyspark.sql import functions as Func


def process():
    """
    This function reads all files specified in /airflow/airflow_file_info.json, clean the data, and write to Postgres
    :type
    :rtype
    """
    json_file = 's3a://data-harbor/safetyinfo/airflow/airflow_file_info.json'
    spark = create_spark_session('process-airflow-update')

    with open(json_file, 'r') as f:
        data = json.load(f)
        for file in data['file']:
            filename = file['file_name']
            city_name = file['city_name']
            id_column_name = file['id_column_name']
            occur_date_column_name = file['occur_date_column_name']
            description_column_name = file['description_column_name']
            info_type = file['info_type']
            latitude_column_name = file['latitude_column_name']
            longitude_column_name = file['longitude_column_name']
            header = file['header']
            delimiter = file['delimiter']
            from_date = file['from_date']
            to_date = file['to_date']

            source = 's3a://data-harbor/safetyinfo/airflow/' + filename
            select_columns = "\'" + id_column_name + "\', \'" + occur_date_column_name + "\', \'" + description_column_name \
                             + "\', \'" + city_name + "\', \'" + info_type \
                             + "\', \'" + latitude_column_name +  "\', \'" + longitude_column_name + "\'"

            df = spark.read.format('com.databricks.spark.csv') \
                .options(header=header) \
                .options(delimiter=delimiter) \
                .options(quote='"') \
                .options(escape='"') \
                .select_columns(select_columns)\
                .load(source)
            df.createOrReplaceTempView("incidents")

            sql = "SELECT DISTINCT " + id_column_name + " as ID, " + occur_date_column_name + " as occur_date, "\
                             + description_column_name + " as description, " + city_name + " as city," + info_type + " as type, "\
                             + latitude_column_name + " as latitude, " + longitude_column_name + " as longitude"
            df_incident = spark.sql(sql)
            df = df.withColumn('ID', df.ID.cast('INT'))
            df = df.withColumn('occur_date', Func.to_date(df.occur_date))
            df = df.withColumn('latitude', df.latitude.cast('FLOAT'))
            df = df.withColumn('longitude', df.longitude.cast('FLOAT'))
            df.createOrReplaceTempView("incidents")
            sql = " SELECT * FROM incidents WHERE latitude is not null AND longitude is not null" \
                + " AND occur_date >= " + from_date + " AND occur_date <= " + to_date
            df_incident = spark.sql(sql)

            write_to_postgres(df_incident, 'safety_info_' + city_name, 'append')


def is_json_file_exists():
    s3 = boto3.resource('s3')
    s3_bucket = 'data-harbor'
    s3_folder = 'safetyinfo/airflow/'
    s3_filename = 'airflow_file_info.json'
    try:
        s3.Object(s3_bucket + s3_folder + s3_filename).load()
    except ClientError as e:
        return False
    return True


if __name__ == '__main__':
    assert is_json_file_exists(), "Json file doesn't exit."
    process()
