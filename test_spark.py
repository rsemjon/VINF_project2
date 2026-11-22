"""Connectivity test: connect to the Spark container via Spark Connect and build a dummy DataFrame.

Requires:  pip install "pyspark[connect]"   (same major version as the Spark image)
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
print("Spark version:", spark.version)

df = spark.createDataFrame(
    [(1, "alice", 3.5), (2, "bob", 4.0), (3, "carol", 2.5)],
    ["id", "name", "score"],
)
df.show()
print("rows:", df.count())

spark.stop()



