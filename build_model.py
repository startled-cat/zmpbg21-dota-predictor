from pyspark.ml import feature, classification, evaluation, Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from tqdm import tqdm

characterCount = 113
schema = StructType([
    StructField("team_win", IntegerType(),  True),
    StructField("cluster_id", IntegerType(), True),
    StructField("game_mode", IntegerType(), True),
    StructField("game_type", IntegerType(), True),
    *[StructField(f"character_{x}",  IntegerType(), True)
      for x in range(characterCount+1)],
])

# build spark context
print("========== building spark context")
spark = SparkSession.builder.appName("ml").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

# load data
print("========== loading data from file")
df_train = spark.read.csv("./dota_data/dota2Train.csv",
                          header=False, schema=schema)
df_eval = spark.read.csv("./dota_data/dota2Test.csv",
                         header=False, schema=schema)
maxDepth = 27
instances = 57
trees = 14
bins = 4

# print(f"\nminInstancesPerNode = {instances}")
# build ml model
# print("========== building ml model")
idx = feature.StringIndexer(inputCol="team_win", outputCol="label",
                            stringOrderType="alphabetAsc", handleInvalid="keep")
vect = feature.VectorAssembler(
    inputCols=df_train.columns[4:], outputCol="features", handleInvalid="keep")
# scaler = feature.StandardScaler(inputCol="feat", outputCol="features")


forest = classification.RandomForestClassifier(
    maxDepth=maxDepth, minInstancesPerNode=instances, maxBins=bins, maxMemoryInMB=8192, numTrees=trees, seed=42)
pipe = Pipeline(stages=[idx, vect, forest])
pipe_t = pipe.fit(df_train)

# save model to file
print("========== saving ml model")
pipe_t.save("./model")

pipe_t2 = pipe_t.transform(df_eval)#.drop(*df_eval.columns[1:])
evaluator = evaluation.MulticlassClassificationEvaluator(metricName="accuracy")
a = evaluator.evaluate(pipe_t2)
print(f"df_eval accuracy:  {a*100:.2f}%")

pipe_t2 = pipe_t.transform(df_train)#.drop(*df_eval.columns[1:])
evaluator = evaluation.MulticlassClassificationEvaluator(metricName="accuracy")
a = evaluator.evaluate(pipe_t2)
print(f"df_train accuracy:  {a*100:.2f}%")

