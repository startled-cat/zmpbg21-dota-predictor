from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.ml import PipelineModel
from pyspark.sql.types import (
    IntegerType,
    StructField,
    StructType,
)



KAFKA_TOPIC_NAME = "topicBD"
KAFKA_TOPIC_NAME2 = "topicBD2"
KAFKA_BOOTSTRAP_SERVER = "127.0.0.1:9092"
characterCount = 113


cls_cols = [
    "team_win",
    "cluster_id",
    "game_mode",
    "game_type",
    *[f"character_{x}" for x in range(1, characterCount+1)],
]
message_schema = StructType([
    StructField("team_win", IntegerType(),  True),
    StructField("cluster_id", IntegerType(), True),
    StructField("game_mode", IntegerType(), True),
    StructField("game_type", IntegerType(), True),
    *[StructField(f"character_{x}",  IntegerType(), True)
      for x in range(1, characterCount+1)],
])



def main():
    print("========== building spark context")
    spark = SparkSession.builder.appName("ml").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    print("========== loading model")
    pipe_t = PipelineModel.load("./model")

    print("========== opening kafka stream")
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("includeHeaders", "true")
        .load()
    )
    parsed = stream.select(
        f.from_json(stream.value.cast("string"), message_schema).alias("json")
    ).select("json.*")

    print("========== opening fit stream")
    temp1 = (
        pipe_t.transform(parsed.select(cls_cols))
        .select(["prediction", "features"])
        # .withColumnRenamed("prediction", "value")
        # .selectExpr("CAST(value as STRING)")
    )
    print("========== opening console stream")
    
    console = (
        temp1.writeStream.outputMode("append")
        .option("truncate", "false")
        .format("console")
        .start()
    )
    
    console.awaitTermination()
    

if __name__ == "__main__":
    main()
