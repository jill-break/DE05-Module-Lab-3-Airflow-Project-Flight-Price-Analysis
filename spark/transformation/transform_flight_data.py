from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, desc, concat, lit

spark = SparkSession.builder \
    .appName("FlightPriceAnalysis_Gold") \
    .getOrCreate()

# 1. Load from MySQL (Silver Table)
mysql_url = "jdbc:mysql://mysql:3306/mysql_db"
mysql_props = {"user": "mysql_user", "password": "mysql_pass", "driver": "com.mysql.cj.jdbc.Driver"}
df = spark.read.jdbc(url=mysql_url, table="flight_prices_clean", properties=mysql_props)

# 2. KPI: Average Fare and Booking Count by Airline
airline_kpis = df.groupBy("airline").agg(
    avg("total_fare_bdt").alias("avg_fare"),
    count("*").alias("booking_count")
)

# 3. KPI: Seasonal Fare Variation
# Requirement: Define peak seasons (Eid, Winter) and compare to non-peak
df_seasonal = df.withColumn("season_category", 
    when(col("seasonality").contains("Winter") | col("seasonality").contains("Eid"), "Peak")
    .otherwise("Non-Peak")
)
seasonal_kpis = df_seasonal.groupBy("season_category").agg(avg("total_fare_bdt").alias("avg_seasonal_fare"))

# 4. KPI: Most Popular Routes
# Identify top source-destination pairs by booking count
popular_routes = df.groupBy("source", "destination") \
    .agg(count("*").alias("route_booking_count")) \
    .orderBy(desc("route_booking_count"))

# 5. Write results to PostgreSQL Analytics DB
pg_url = "jdbc:postgresql://postgres_analytics:5432/psql_db"
pg_props = {"user": "psql_user", "password": "psql_pass", "driver": "org.postgresql.Driver"}

airline_kpis.write.jdbc(url=pg_url, table="gold_airline_performance", mode="overwrite", properties=pg_props)
seasonal_kpis.write.jdbc(url=pg_url, table="gold_seasonal_analysis", mode="overwrite", properties=pg_props)
popular_routes.write.jdbc(url=pg_url, table="gold_popular_routes", mode="overwrite", properties=pg_props)

spark.stop()