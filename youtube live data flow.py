# Databricks notebook source
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/youtube/schema/") \
    .option("multiline", "true") \
    .load("/mnt/govinda/Data flow live/")
df.display()

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import *

# Define the schema of the JSON `items` array
video_schema = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("snippet", StructType([
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("publishedAt", StringType()),
        StructField("channelId", StringType()),
        StructField("channelTitle", StringType()),
        StructField("defaultLanguage", StringType()),
        StructField("thumbnails", StructType([
            StructField("high", StructType([
                StructField("url", StringType())
            ]))
        ])),
        StructField("tags", ArrayType(StringType()))
    ])),
    StructField("statistics", StructType([
        StructField("viewCount", StringType()),
        StructField("likeCount", StringType()),
        StructField("commentCount", StringType())
    ]))
]))

# Parse the `items` column from string to array of structs
df_parsed = df.withColumn("parsed_items", from_json(col("items"), video_schema))

# Explode the array of videos
df_exploded = df_parsed.selectExpr("explode(parsed_items) as video")

# Final selection in your requested format
df2 = df_exploded.select(
    col("video.id").alias("video_id"),
    col("video.snippet.title").alias("title"),
    col("video.snippet.description").alias("description"),
    col("video.snippet.publishedAt").alias("published_at"),
    col("video.snippet.channelId").alias("channel_id"),
    col("video.snippet.channelTitle").alias("channel_title"),
    col("video.snippet.defaultLanguage").alias("default_language"),
    col("video.snippet.thumbnails.high.url").alias("thumbnail_url"),
    col("video.snippet.tags").alias("tags"),
    col("video.statistics.viewCount").cast("long").alias("views"),
    col("video.statistics.likeCount").cast("long").alias("likes"),
    col("video.statistics.commentCount").cast("long").alias("comments")
)


# COMMAND ----------

display(df2)

# COMMAND ----------

df3 = df2.select("video_id", "published_at", "channel_title", "default_language", "views", "likes", "comments")
df3.display()

# COMMAND ----------

from pyspark.sql.functions import col

df4 = df3.filter(col("default_language") == "en") \
         .groupBy("default_language") \
         .count()
display(df4)


# COMMAND ----------

from pyspark.sql.functions import when, col, lower

df_cleaned = df3.withColumn(
    "cleaned_language",
    when(col("default_language").isNull(), "regional Language")
    .when(lower(col("default_language")).like("en%"), "en")
    .when(lower(col("default_language")).like("or%"), "or")
    .otherwise(lower(col("default_language")))
)


# COMMAND ----------

df_cleaned.display()


# COMMAND ----------

df5 = df_cleaned.select("video_id", "published_at", "channel_title", "cleaned_language", "views", "likes", "comments")
df5.display()

# COMMAND ----------

df5.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints/youtube/silver/") \
    .outputMode("append") \
    .start("/mnt/delta/youtube/silver/")




# COMMAND ----------

dbutils.fs.ls("/mnt/delta/youtube/silver/")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.youtube_most_popular
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/delta/youtube/silver/';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.youtube_most_popular;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import max

df_final = df5 \
    .groupBy("cleaned_language", "channel_title") \
    .agg(max("views").alias("max_views"))

df_final.display()


# COMMAND ----------

df_gold_read = spark.read.format("delta").load("/mnt/delta/youtube/silver/")

df_filtered = df_gold_read \
    .groupBy("cleaned_language", "channel_title") \
    .agg(max("views").alias("max_views"))

df_filtered.display()


# COMMAND ----------

df_filtered.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/govinda/delta1/youtube/gold_lang_channel/")



# COMMAND ----------

df_filtered.write.format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("/mnt/govinda/csv/youtube/gold_lang_channel/")


# COMMAND ----------

df_filtered.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_lang_channel")