# ToneBnb
This is a project I completed during the Insight Data Engineering program (Silicon Valley, Fall 2019). 
Visit dataharbor.club to see it in action.

This projet built a data pipeline ingesting  history incidents/complain information and 
airbnb listing & review data, visualized the resuls through flask on Google map, so the 
 user can book informed.

The background is a heatmap generated from aggregated data of a specific city's public incidents and complains 
datasets. It presents the safety related information of a location. In this project, it's called "The Tone of a 
Location". The Airbnb listings' color is based on the adjusted rating & review score. The brighter the color is, 
the higher the score; It's called "The Tone of a House". When a listing is clicked, all incidents happened around 
within a specific time window and scope will display.
![Image description](docs/overall.png)
![Image description](docs/happened_arround.png)

## Pipeline
All the raw data were uploaded to S3 and cleaned by spark then saved back to S3 in Parquet format. 
These clean data can be reused to feed to spark computation job and tune the parameters. After computation, the result 
was saved to postGIS, which has convenient Geo functions. And finally, the results will be visualized through Flask 
and Google map api.


![Image description](docs/pipe_line.jpeg)

## Engineering Challenges
1. Schema serialization: Safety data schemas are totally different

    How to solve -- Use a Json file Encoding the schema, then the pipeline decodes schema from Json file, 
    so all kinds of safety info 
can be ingested without recoding. 
2. Heatmap: There are millions of safety related records, which are impossible for UI to display. 

    How to solve: Use GeoHash to aggregate safety info into blocks. After aggregation, 
    there are 5k~6k blocks records with weight, a reasonable number to feed to javascript.

## Dataset
San Francisco Incidents (2003~present): 2.47m records
2003~2018: 2.21m rows, 13 columns
January 1, 2018 ~Present: 265k rows, 26 columns

San Francisco 311 Complaints (2003~present): 3.79m records
July 2008~present: 3.79m rows, 20 columns

Airbnb history data (35 available cities, U.S. & Canada):~100gb
SF, LA, NY, Auston, Seattle, Boston, Chicago, Oakland, Denver, Hawaii, Portland, San Diego, Santa Clara, Toronto, 
Vancouver, Washington DC... 


## Cluster Structure:
To reproduce my environment, 4 m4.large AWS EC2 instances are needed:

(4ß nodes) Spark Cluster - Batch

PostgreSQL sits in Mater's node

Flask is in Master's node


## Caveat
At this time of writing, pyspark can work with only Java 8. Make sure spark env is Java 8.
