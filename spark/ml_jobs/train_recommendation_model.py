from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS

spark = SparkSession.builder.appName("MLTrainer").getOrCreate()

# Lire les données Gold depuis MinIO
df = spark.read.parquet("s3a://gold/offres_enrichies/")

# Entraîner un modèle ALS (Alternating Least Squares)
als = ALS(maxIter=10, regParam=0.01,
          userCol="user_id", itemCol="offre_id", ratingCol="score")
model = als.fit(df)

# Sauvegarder dans MinIO
model.save("s3a://models/recommendation_model/")
spark.stop()