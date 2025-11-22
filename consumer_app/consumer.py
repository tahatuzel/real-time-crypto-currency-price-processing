import os
import redis
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, DecimalType, BooleanType
from pyspark.sql.window import Window


def process_data(df, batch_id):
    spark = df.sparkSession
    
    r_client = redis.Redis(host='redis', port=6379, db=0)

    new_data_df = df.select(
        (sf.col("timestamp")/1000).cast(TimestampType()).alias("event_time"),
        sf.regexp_replace(sf.col("symbol"),"USDT","").alias("symbol"),
        sf.round(sf.col("price").cast(DoubleType()),3).alias("price_usd"),        
    )
    
    
    new_data_df.cache() 

    if new_data_df.count() > 0:
        symbols_in_batch = [row.symbol for row in new_data_df.select("symbol").distinct().collect()]
        
        old_prices = []
        if symbols_in_batch:
            redis_values = r_client.mget(symbols_in_batch)
            
            for symbol, price in zip(symbols_in_batch, redis_values):
                if price:

                    old_prices.append((symbol, float(price), True))
        
        schema_history = StructType([
            StructField("symbol", StringType()),
            StructField("price_usd", DoubleType()),
            StructField("is_history", BooleanType())
        ])

        if old_prices:
            history_data_df = spark.createDataFrame(old_prices, schema_history)
            history_data_df = history_data_df.withColumn("event_time", sf.lit(None).cast(TimestampType()))
        else:
            history_data_df = spark.createDataFrame([], schema_history).withColumn("event_time", sf.lit(None).cast(TimestampType()))

        new_data_with_flag = new_data_df.withColumn("is_history", sf.lit(False))
        
        history_data_df = history_data_df.select("event_time", "symbol", "price_usd", "is_history")
        combined_data_df = history_data_df.unionByName(new_data_with_flag)

        w = Window.partitionBy("symbol").orderBy(sf.col("is_history").desc(), sf.col("event_time"))
        
        calculated_df = combined_data_df.withColumn("old_price_usd", sf.lag("price_usd", 1).over(w))
        
        calculated_df = calculated_df.withColumn("exchange_rate", 
                            sf.when(( (sf.col("old_price_usd").isNotNull()) & (sf.col("old_price_usd")!=0) ), 
                                    (sf.col("price_usd") - sf.col("old_price_usd")) / sf.col("old_price_usd"))
                            .otherwise(0)
                           )

        final_df = calculated_df.filter(sf.col("is_history") == False).drop("is_history")

        db_url = f"jdbc:postgresql://project_db:{os.getenv('APP_DB_PORT')}/{os.getenv('APP_DB_NAME')}"
        db_properties = {"user": os.getenv("APP_DB_USER"), "password": os.getenv("APP_DB_PASS"), "driver": "org.postgresql.Driver"}
        
        final_df.write.jdbc(url=db_url, table="staging.raw_prices", mode="append", properties=db_properties)


        w_last = Window.partitionBy("symbol").orderBy("event_time")
        
        latest_prices_df = new_data_df.withColumn("row_num", sf.row_number().over(w_last.orderBy(sf.col("event_time").desc()))) \
                                      .filter(sf.col("row_num") == 1) \
                                      .select("symbol", "price_usd")
        
        updates = latest_prices_df.collect()
        
        redis_update_dict = {row.symbol: str(row.price_usd) for row in updates}
        
        if redis_update_dict:
            r_client.mset(redis_update_dict)

    new_data_df.unpersist()

    

def main():
    spark = SparkSession.builder \
        .appName("KafkaPySparkStreaming") \
        .getOrCreate()
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_prices") \
        .option("startingOffsets", "earliest") \
        .load()

    json_schema = StructType([
        StructField("timestamp", LongType()),
        StructField("symbol", StringType()),
        StructField("price", StringType())  
        
    ])

    parsed_df = df.select(sf.col("value").cast("string")) \
                  .select(sf.from_json(sf.col("value"), json_schema).alias("data")) \
                  .select("data.*")


    query = parsed_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_data) \
        .trigger(processingTime="15 seconds") \
        .start()
        
    
    
    query.awaitTermination()

if __name__ == "__main__":
    main()