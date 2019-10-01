from pyspark.sql.types import *

AIRBNB_LISTING_SCHEMA = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('host_id', StringType(), True),
    StructField('host_name', StringType(), True),
    StructField('neighbourhood_group', StringType(), True),
    StructField('neighbourhood', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('room_type', StringType(), True),
    StructField('price', StringType(), True),
    StructField('minimum_nights', StringType(), True),
    StructField('number_of_reviews', StringType(), True),
    StructField('last_review', StringType(), True),
    StructField('reviews_per_month', StringType(), True),
    StructField('calculated_host_listings_count', StringType(), True),
    StructField('availability_365', StringType(), True)
])

INCIDENTS_SCHEMA_2003_SF = StructType([
    StructField('incident_number', StringType(), True),
    StructField('category', StringType(), True),
    StructField('incident_description', StringType(), True),
    StructField('incident_day_of_week', StringType(), True),
    StructField('incident_date', StringType(), True),
    StructField('incident_time', StringType(), True),
    StructField('police_district', StringType(), True),
    StructField('resolution', StringType(), True),
    StructField('address', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('point', StringType(), True),
    StructField('pd_id', StringType(), True)
])

INCIDENTS_SCHEMA_SF = StructType([
    StructField('incident_datetime', StringType(), True),
    StructField('incident_date', StringType(), True),
    StructField('incident_time', StringType(), True),
    StructField('incident_year', StringType(), True),
    StructField('incident_day_of_week', StringType(), True),
    StructField('report_datetime', StringType(), True),
    StructField('row_id', StringType(), True),
    StructField('incident_id', StringType(), True),
    StructField('incident_number', StringType(), True),
    StructField('cad_number', StringType(), True),
    StructField('report_type_code', StringType(), True),
    StructField('report_type_description', StringType(), True),
    StructField('filed_online', StringType(), True),
    StructField('incident_code', StringType(), True),
    StructField('incident_category', StringType(), True),
    StructField('incident_subcategory', StringType(), True),
    StructField('incident_description', StringType(), True),
    StructField('resolution', StringType(), True),
    StructField('intersection', StringType(), True),
    StructField('cnn', StringType(), True),
    StructField('police_district', StringType(), True),
    StructField('analysis_neighborhood', StringType(), True),
    StructField('supervisor_district', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('point', StringType(), True)
])

COMPLAIN_SCHEMA_SF = StructType([
    StructField('caseid', StringType(), True),
    StructField('opened', StringType(), True),
    StructField('closed', StringType(), True),
    StructField('updated', StringType(), True),
    StructField('status', StringType(), True),
    StructField('status_notes', StringType(), True),
    StructField('responsible_agency', StringType(), True),
    StructField('Category', StringType(), True),
    StructField('Request_type', StringType(), True),
    StructField('Request_Details', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('Street', StringType(), True),
    StructField('Supervisor_District', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Police_District', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('point', StringType(), True),
    StructField('Source', StringType(), True),
    StructField('Media_URL', StringType(), True)
])


AIRBNB_REVIEWS_SCHEMA = StructType([
    StructField('listing_id', StringType(), True),
    StructField('id', StringType(), True),
    StructField('date', StringType(), True),
    StructField('reviewer_id', StringType(), True),
    StructField('reviewer_name', StringType(), True),
    StructField('comments', StringType(), True),
    StructField('tone', StringType(), True)
])
