from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, desc

# Initialize Spark Session
jars_path = "/opt/jars/mysql-connector-j-9.5.0.jar,/opt/jars/postgresql-42.7.6.jar"
spark = SparkSession.builder \
    .appName("FlightPriceAnalysis_Gold") \
    .master("local[*]") \
    .config("spark.jars", jars_path) \
    .config("spark.driver.extraClassPath", jars_path.replace(",", ":")) \
    .getOrCreate()

# 1. Load from MySQL (Silver Table: flight_prices_clean)
mysql_url = "jdbc:mysql://mysql:3306/mysql_db"
mysql_props = {
    "user": "mysql_user", 
    "password": "mysql_pass", 
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read the cleaned data from the Silver layer
df = spark.read.jdbc(url=mysql_url, table="flight_prices_clean", properties=mysql_props)

# 2. KPI: Average Fare and Booking Count by Airline
# Requirement: Compute average fare and total bookings per airline
airline_performance = df.groupBy("airline").agg(
    avg("total_fare_bdt").alias("avg_fare"),
    count("*").alias("booking_count")
)

# 3. KPI: Seasonal Fare Variation
# Requirement: Define peak seasons (Eid, Winter) and compare to non-peak
# Used rlike for case-insensitive matching of key holiday terms
peak_pattern = "Winter|Eid|Holiday"
df_seasonal = df.withColumn("season_category", 
    when(col("seasonality").rlike(peak_pattern), "Peak")
    .otherwise("Non-Peak")
)

seasonal_analysis = df_seasonal.groupBy("season_category").agg(
    avg("total_fare_bdt").alias("avg_seasonal_fare")
)

# 4. KPI: Most Popular Routes
# Requirement: Identify top source-destination pairs by booking count
popular_routes = df.groupBy("source", "destination") \
    .agg(count("*").alias("route_booking_count")) \
    .orderBy(desc("route_booking_count"))

# 5. Write results to PostgreSQL Analytics DB (Gold Layer)
pg_url = "jdbc:postgresql://postgres_analytics:5432/psql_db"
pg_props = {
    "user": "psql_user", 
    "password": "psql_pass", 
    "driver": "org.postgresql.Driver"
}

# Overwrite mode ensures idempotency for daily runs
airline_performance.write.jdbc(url=pg_url, table="gold_airline_performance", mode="overwrite", properties=pg_props)
seasonal_analysis.write.jdbc(url=pg_url, table="gold_seasonal_analysis", mode="overwrite", properties=pg_props)
popular_routes.write.jdbc(url=pg_url, table="gold_popular_routes", mode="overwrite", properties=pg_props)

print("Gold Layer KPIs successfully loaded to PostgreSQL.")
spark.stop()