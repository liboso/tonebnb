# Tonebnb
This is a project I completed during the Insight Data Engineering program (Silicon Valley, Fall 2019). 
Visit dataharbor.club to see it in action.

This project aims at providing the history safety related information of locations and adjusted rating scores of 
airbnb listings to travellers，so they can book fast and informed.

The background of the UI is a heatmap with aggregated data from the specific city's public incidents and complains 
datasets, it presents the safety related information of a location, in this project, it's called "The Tone of a 
Location"; The Airbnb listings' color are changed according to the adjusted score, the brighter th color, the higher 
the score; It's called "The Tone of a House". When a listing is clicked, all incidents happened around within a 
specific time window and scope will display.
![Image description](docs/overall.png)
![Image description](docs/happened_arround.png)

## Pipeline
Clean Job:  All the raw data were downloaded to S3 and cleaned by spark then saved back to S3 in Parquet format. 
These clean data can be reused to feed to spark computation job and tune the parameters. After computation, the result 
was saved to postGIS, which has convenient Geo functions. And finally, the results will be visualized through Flask 
and Google map api.


![Image description](docs/pipe_line.jpeg)
## Dataset
San Francisco Incidents (2003~present): 2.47m records
2003~2018: 2.21m rows, 13 columns
January 1, 2018 ~Present: 265k rows, 26 columns

San Francisco 311 Complaints (2003~present): 3.79m records
July 2008~present: 3.79m rows, 20 columns

Airbnb history data (35 available cities, U.S. & Canada):~100gb
SF, LA, NY, Auston, Seattle, Boston, Chicago, Oakland, Denver, Hawaii, Portland, San Diego, Santa Clara, Toronto, Vancouver, Washington DC... 


## Cluster Structure:
To reproduce my environment, 6 m4.large AWS EC2 instances are needed:

(6 nodes) Spark Cluster - Batch

PostgreSQL sits in Mater's node

Flask is in Mater's node

## Access S3 from Spark
Add the following configuration in `$SPARK_HOME/conf/spark-defaults.conf`
```ini
spark.hadoop.fs.s3a.access.key=MY_ACCESS_KEY
spark.hadoop.fs.s3a.secret.key=MY_SECRET_KEY
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Caveat
- At this time of writing, pyspark can work with only Java 8. Make sure spark env is Java 8.
