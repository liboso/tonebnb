import os
import platform
import geohash
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '10.0.0.5')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'tonebnb')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'sa')
POSTGRES_PWD = os.getenv('POSTGRES_PWD', 'sa')
POSTGRES_URL = f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'


def write_to_postgres(df, table, mode):
    DataFrameWriter(df).jdbc(POSTGRES_URL, table, mode, {
        'user': POSTGRES_USER,
        'password': POSTGRES_PWD,
        'driver': 'org.postgresql.Driver'
    })


def _geo_encode(x, y, precision):
    try:
        return geohash.encode(float(x), float(y), precision)
    except:
        return ""


def create_spark_session(app=None, mem=None):
    builder = SparkSession.builder.appName(app)
    if mem:
        builder.config('spark.executor.memory', mem)
    elif platform.system() == 'Linux':  # Default spark worker on EC2 is 6GB
        builder.config('spark.executor.memory', '6gb')

    _spark = builder.getOrCreate()

    # Ensure python dependency files be available on spark driver/workers
    for dirpath, dirnames, filenames in os.walk(os.path.dirname(os.path.realpath(__file__))):
        for file in filenames:
            if file.endswith('.py'):
                _spark.sparkContext.addPyFile(os.path.join(dirpath, file))

    return _spark
