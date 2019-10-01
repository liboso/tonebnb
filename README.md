# Toned Airbnb
A project for Insight Fellow program

## Access S3 from Spark
Add the following configuration in `$SPARK_HOME/conf/spark-defaults.conf`
```ini
spark.hadoop.fs.s3a.access.key=MY_ACCESS_KEY
spark.hadoop.fs.s3a.secret.key=MY_SECRET_KEY
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```
Add the following 3rd party library into `$SPARK_HOME/lib/`
```bash
$ cd $SPARK_HOME/lib/
$ wget ...
```

## Caveat
- At this time of writing, pyspark can work with only Java 8. Make sure spark env is Java 8.
