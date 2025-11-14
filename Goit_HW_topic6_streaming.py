from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, struct, to_json, lit

from Goit_HW_topic6_configs import kafka_config, building_sensors, temperature_alerts, humidity_alerts


# створення spark session
spark = SparkSession.builder.appName("BuildingSensorsStreaming").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 4)

# Читання CSV-файлу з умовами
path_to_uploaded = "/Users/oie/Documents/my_pyspark_repository/kafka_streaming/alerts_conditions.csv"
alerts_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(path_to_uploaded)

print("Alerts conditions:")
alerts_df.show()

# Читання потокових даних
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
    .option("subscribe", building_sensors) \
    .option("startingOffsets", "earliest") \
    .load()

# Визначення схеми для JSON
schema = StructType([
    StructField("sensor_building_id", IntegerType()),
    StructField("timestamp", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

# Обробка даних
parsed_df = df.select(
    col("key").cast("string").alias("sensor_id"), 
    from_json(col("value").cast("string"), schema).alias("data")
)

processed_df = parsed_df.select(
    col("sensor_id"),
    "data.temperature",
    "data.humidity",
    to_timestamp(col("data.timestamp")).alias("event_time")
)

df_with_watermark = processed_df.withWatermark("event_time", "10 seconds")

windowed_avg_df = df_with_watermark.groupBy(
    window(
        timeColumn="event_time",
        windowDuration="1 minute",
        slideDuration="30 seconds"
    ),
    col("sensor_id")
).agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
)

# Приєднання з умовами з CSV
joined_df = windowed_avg_df.crossJoin(alerts_df)

# Фільтрування, щоб знайти спрацювання алертів
# Ігноруємо правила, де значення -999
triggered_alerts_df = joined_df.where(
    ( (col("temperature_max") != -999) & (col("avg_temperature") > col("temperature_max")) ) |
    ( (col("temperature_min") != -999) & (col("avg_temperature") < col("temperature_min")) ) |
    ( (col("humidity_max") != -999) & (col("avg_humidity") > col("humidity_max")) ) |
    ( (col("humidity_min") != -999) & (col("avg_humidity") < col("humidity_min")) )
)

# Фррмування коду алерту
alert_struct = struct(col("message"), col("code"))

payload_struct = struct(
    col("window"), 
    col("sensor_id"),
    col("avg_temperature"), 
    col("avg_humidity"), 
    alert_struct.alias("alert")
)

# Створення 'value' (JSON) та 'key' (ID сенсора) для Kafka
output_df = triggered_alerts_df.select(
    col("sensor_id").alias("key"),
    to_json(payload_struct).alias("value"),
    col("code")
)

# Запис у Kafka за допомогою foreachBatch
# Ця функція буде обробляти кожен мікро-батч
def write_alerts_to_kafka(batch_df, batch_id):
    print(f"--- Batch processing: {batch_id} ---")
    
    batch_df.persist()

    temp_alerts = batch_df.filter((col("code") == 103) | (col("code") == 104)) \
                          .select("key", "value")
    
    temp_count = temp_alerts.count()
    if temp_count > 0:
        print(f"Send {temp_count} alerts for temperature...")
        temp_alerts.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("topic", temperature_alerts) \
            .save()

    hum_alerts = batch_df.filter((col("code") == 101) | (col("code") == 102)) \
                         .select("key", "value")
    
    hum_count = hum_alerts.count()
    if hum_count > 0:
        print(f"Send {hum_count} alerts for humidity...")
        hum_alerts.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("topic", humidity_alerts) \
            .save()

    batch_df.unpersist()


# Старт стріму та запис у Kafka
query = output_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_alerts_to_kafka) \
    .start()

print("Streaming started... Waiting for data...")
query.awaitTermination()
