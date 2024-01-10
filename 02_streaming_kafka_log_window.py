from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

import pyspark.sql.functions as F

if __name__  == "__main__":
    spark = (
        SparkSession.builder.appName("stream-kafka").config("spark.sql.streaming.stateStore.stateSchemaCheck", "false").getOrCreate()
    )
    
    schema = StructType(
        [
            StructField("host", StringType(), True),
            StructField("user-identifier", StringType(), True),
            StructField("datetime", TimestampType(), True),
            StructField("method", StringType(), True),
            StructField("request", StringType(), True),
            StructField("protocol", StringType(), True),
            StructField("status", IntegerType(), True),
            StructField("bytes", IntegerType(), True),
            StructField("referer", StringType(), True),
        ]
    )
    
    # kafka와 spark를 이어주는 데이터 프레임 정의. Spark가 Consumer의 역할을 함.
    logs_df = (
        spark.readStream.format("kafka") # 포멧이 socket에서 kafka로 변경
            .option("kafka.bootstrap.servers", "172.31.36.132:9092") # 브로커 지정
            .option("subscribe","sample-logs") # 토픽 지정
            .option("startingOffsets", "earliest") # Consume을 시작할 위치. earliest로 설정하면 읽어오지 않은 토픽부터 읽어옴
            .option("failOnDataLoss", "false") # 데이터가 문제가 생기면 종료. false로 설정.
            .load()
    )
    
    # logs_df.writeStream.format("console").outputMode("append").start().awaitTermination()
    
    # Kafka토픽에 있는 데이터를 Byte Array 형식임. 이 데이터를 문자열로 바꾼 다음 json으로 변환하면 됨. -> Decode
    value_df = (
        logs_df.withColumn(
                "value", 
                F.from_json(F.col("value").cast("string"), schema = schema))
            .select(F.col("value"))
            .selectExpr("value.*")
    )
    
    window_by_hours = (
        value_df.withWatermark("datetime", "5 seconds") # 지금 없어도 될 듯하지만 
            .groupby(F.window(F.col("datetime"),"5 seconds").alias("time"))
            .agg(F.sum("bytes").alias("total_bytes"))
            .select(
                F.col("time").getField("start").alias("start"),
                F.col("time").getField("end").alias("end"),
                F.col("total_bytes")
            )
    )
    # window_by_hours.writeStream.format("console").outputMode("complete").start().awaitTermination()
    
    (
        window_by_hours.writeStream
            .format("parquet")
            .option("path", "/home/ubuntu/working/spark-examples/spark-streaming/kafka-spark-streaming/output")
            .option("checkpointLocation", ".checkpoint")
            .trigger(processingTime="5 seconds") # 해줘야함. 마이크로 배치의 생성 간격 (5초마다 한번씩 마이크로 배치의 내용을 저장)
            .outputMode("append") # 파일로 데이터 저장시에는 append모드만 지원 (complete모드 에러)
            .start().awaitTermination()
    )
    
    
    