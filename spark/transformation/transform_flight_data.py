from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FlightPriceKPIs") \
    .get_raw_data() \
    .getOrCreate()

# 1. Read validated data from MySQL (Silver Layer)
# Using the connection details from your docker-compose
mysql_url = "jdbc:mysql://mysql:3306/mysql_db"
mysql_properties = {"user": "mysql_user", "password": "mysql_pass", "driver": "com.mysql.cj.jdbc.Driver"}

df = spark.read.jdbc(url=mysql_url, table="flight_prices_clean", properties=mysql_properties)

# 2. Compute KPIs
# KPI: Average Fare by Airline
avg_fare_df = df.groupBy("Airline").agg(avg("Total Fare (BDT)").alias("avg_fare"))

# KPI: Seasonal Fare Variation
# Define Peak Seasons (e.g., Winter holidays)
seasonal_df = df.withColumn("season_type", 
    when(col("Seasonality") == "Winter", "Peak").otherwise("Non-Peak")
).groupBy("season_type").agg(avg("Total Fare (BDT)").alias("avg_seasonal_fare"))

# 3. Write to PostgreSQL (Gold Layer)
pg_url = "jdbc:postgresql://postgres_analytics:5432/psql_db"
pg_properties = {"user": "psql_user", "password": "psql_pass", "driver": "org.postgresql.Driver"}

avg_fare_df.write.jdbc(url=pg_url, table="kpi_airline_avg", mode="overwrite", properties=pg_properties)
seasonal_df.write.jdbc(url=pg_url, table="kpi_seasonal_variation", mode="overwrite", properties=pg_properties)

spark.stop()