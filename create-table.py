# Databricks notebook source
# MAGIC %md
# MAGIC ### small data - all types

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists delta_sharing_r;
# MAGIC create database if not exists delta_sharing_r.simple;

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# TODO
# interval types (complex)
# add special values

# COMMAND ----------

standardSchema = StructType([
  StructField("TimestampType",              TimestampType(),                                True),
  StructField("DateType",                   DateType(),                                     True),
  StructField("StringType",                 StringType(),                                   True),
  StructField("IntegerType",                IntegerType(),                                  True),
  StructField("BooleanType",                BooleanType(),                                  True),
  StructField("FloatType",                  FloatType(),                                    True),
  StructField("DoubleType",                 DoubleType(),                                   True),
  StructField("DecimalType",                DecimalType(5, 2),                              True),
  StructField("LongType",                   LongType(),                                     True),
  StructField("ShortType",                  ShortType(),                                    True),
  StructField("ByteType",                   ByteType(),                                     True),
  StructField("BinaryType",                 BinaryType(),                                   True),
  StructField("ArrayTypeString",            ArrayType(StringType()),                        True),
  StructField("ArrayTypeInteger",           ArrayType(IntegerType()),                       True),
  StructField("ArrayTypeBoolean",           ArrayType(BooleanType()),                       True),
  StructField("ArrayTypeTimestamp",         ArrayType(TimestampType()),                     True),
  StructField("ArrayTypeArrayTypeString",   ArrayType(ArrayType(StringType())),             True),
  StructField("MapTypekStringvString",      MapType(StringType(), StringType()),            True),
  StructField("MapTypekIntegervString",     MapType(IntegerType(), StringType()),           True),
  StructField("MapTypekStringvArrayString", MapType(StringType(), ArrayType(StringType())), True),
  StructField("StructType", StructType([
    StructField("StructFieldA", StringType(), True),
    StructField('StructFieldB', StringType(), True),
    StructField('StructFieldC', StringType(), True)
  ]), True),
  StructField("NestedStructType", StructType([
    StructField("StructFieldA", StructType([
      StructField("StructFieldA1", StringType(), True),
      StructField('StructFieldA2', StringType(), True),
      StructField('StructFieldA3', StructType([
        StructField("StructFieldA3.1", IntegerType(), True),
        StructField('StructFieldA3.2', IntegerType(), True),
        StructField('StructFieldA3.3', IntegerType(), True)
      ]), True)
    ]), True),
    StructField('StructFieldB', StringType(), True),
    StructField('StructFieldC', StringType(), True)
  ]), True)
])

standardSchema

# COMMAND ----------

from datetime import datetime
from decimal import Decimal

dataExample1 = [
  (datetime.strptime("2022-06-01 12:01:19", "%Y-%m-%d %H:%M:%S"),
   datetime.strptime("2022-06-01", "%Y-%m-%d"),
   "Hello World",
   1,
   False,
   100.0,
   3.14,
   Decimal(999.99),
   10000000000,
   100,
   12,
   bytearray([2, 3, 5, 7]),
   ["a", "b", "c"],
   [1, 2, 3],
   [True, False, True],
   [datetime.strptime("2022-06-01 12:01:19", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2022-06-02 12:01:19", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2022-06-03 12:01:19", "%Y-%m-%d %H:%M:%S")],
   [["a"], ["b", "c"], ["d", "e", "f"]],
   {"z": "zz", "x": "xx"},
   {1: "zz", 2: "xx"},
   {"fruit": ["peach", "apple", "pear"], "letters": ["a", "b", "c"]},
   ("a", "b", "c"),
   (("a", "b", (1, 2, 3)), "b", "c")
  ),
   (datetime.strptime("2022-07-01 12:01:19", "%Y-%m-%d %H:%M:%S"),
   datetime.strptime("2022-07-01", "%Y-%m-%d"),
   "The quick brown fox jumped over the lazy dog",
   2,
   True,
   101.0,
   2.14,
   Decimal(-999.99),
   20000000000,
   200,
   15,
   bytearray([2, 3, 5, 9]),
   ["d", "e", "f"],
   [4, 5, 6],
   [False, True, False],
   [datetime.strptime("2022-07-01 12:01:19", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2022-07-02 12:01:19", "%Y-%m-%d %H:%M:%S"), datetime.strptime("2022-07-03 12:01:19", "%Y-%m-%d %H:%M:%S")],
   [["z", "y", "x"], ["w", "v"], ["u"]],
   {"a":"aa", "b":"bb"},
   {3: "zz", 4: "xx"},
   {"birds": ["pigeon", "crow"], "veggies": ["carrot", "peas", "corn"]},
   ("d", "e", "f"),
   (("a", "b", (1, 2, 3)), "b", "c")
  )
]
df = spark.createDataFrame(data=dataExample1, schema=standardSchema)
df.printSchema()
display(df)

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_sharing_r.simple.all_types")

# COMMAND ----------

# MAGIC %md
# MAGIC ### simple non-partitioning/partitioning, basic types

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists delta_sharing_r;
# MAGIC create database if not exists delta_sharing_r.simple;

# COMMAND ----------

import pandas as pd
import numpy as np

df = pd.DataFrame({
  "date": pd.date_range(start='1/1/2022', periods=20)
})

df = df.sample(n = 100000, replace=True)

df['group'] = np.random.choice(["a", "b"], df.shape[0])
for i in range(10):
  df['number' + str(i+1)] = np.random.randint(1, 100, df.shape[0])

df

# COMMAND ----------

df = spark.createDataFrame(df)
df.printSchema()
display(df)

# COMMAND ----------

import pyspark.sql.functions as F
df = df.withColumn("date", F.to_date(F.col("date")))
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta_sharing_r.simple.partitioned_date_group

# COMMAND ----------

df.write.partitionBy("date").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_sharing_r.simple.partitioned_date")

# COMMAND ----------

df.write.partitionBy("date", "group").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_sharing_r.simple.partitioned_date_group")

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_sharing_r.simple.no_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize delta_sharing_r.simple.partitioned_date;
# MAGIC optimize delta_sharing_r.simple.partitioned_date_group;
# MAGIC optimize delta_sharing_r.simple.no_partition zorder by (date, group);

# COMMAND ----------

# MAGIC %md
# MAGIC ### change data feed table

# COMMAND ----------

import pandas as pd
import numpy as np

df = pd.DataFrame({
  "date": pd.date_range(start='1/1/2022', periods=365)
})

def generate_sample(source, n=10):
  df = source.sample(n = n, replace=False)
  df['group'] = np.random.choice(["a", "b"], df.shape[0])
  for i in range(10):
    df['number' + str(i+1)] = np.random.randint(1, 100, df.shape[0])
  return df

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists delta_sharing_r;
# MAGIC create database if not exists delta_sharing_r.simple;

# COMMAND ----------

# seed table
df = spark.createDataFrame(generate_sample(df, 5)).withColumn("date", F.to_date(F.col("date")))
df.write.mode("overwrite").saveAsTable("delta_sharing_r.simple.cdf_no_partition")

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table delta_sharing_r.simple.cdf_no_partition set tblproperties (delta.enableChangeDataFeed = true)

# COMMAND ----------

# at random choose to either append, delete, update
import random
from delta.tables import *
from pyspark.sql.functions import *

df = pd.DataFrame({
  "date": pd.date_range(start='1/1/2022', periods=365)
})


def generate_sample(source, n=10):
  df = source.sample(n, replace=False)
  df['group'] = np.random.choice(["a", "b"], df.shape[0])
  for i in range(10):
    df['number' + str(i+1)] = np.random.randint(1, 100, df.shape[0])
  return df


for i in range (100):
  
  operation = random.choice(["append", "delete", "update"])
  sample = generate_sample(df, 5)
  spark_sample = spark.createDataFrame(sample).withColumn("date", F.to_date(F.col("date")))
  
  if operation == "append":
    spark_sample.write.mode("append").saveAsTable("delta_sharing_r.simple.cdf_no_partition")
  elif operation == "delete":
    deltaTable = DeltaTable.forName(spark, 'delta_sharing_r.simple.cdf_no_partition')
    deltaTable.delete(col('date').isin(sample.date.astype("str").to_list()))
  elif operation == "update":
    deltaTable = DeltaTable.forName(spark, 'delta_sharing_r.simple.cdf_no_partition')
    deltaTable.update(
    condition = col('date').isin(sample.date.astype("str").to_list()),
    set = {
      'number1':  lit(999),
      'number2':  lit(999),
      'number3':  lit(999),
      'number4':  lit(999),
      'number5':  lit(999),
      'number6':  lit(999),
      'number7':  lit(999),
      'number8':  lit(999),
      'number9':  lit(999),
      'number10': lit(999)
    }
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta_sharing_r.simple.cdf_no_partition

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python Test Predicates

# COMMAND ----------

# MAGIC %pip install delta-sharing

# COMMAND ----------

import delta_sharing as ds

profile_file = "/dbfs/Users/zacdav/tmp/creds.share"
profile = ds.protocol.DeltaSharingProfile.read_from_file(profile_file)
client = ds.rest_client.DataSharingRestClient(profile=profile)
table = ds.Table(share="deltasharingr", schema="simple", name="partitioned_date_group")

# COMMAND ----------

files = client.list_files_in_table(table=table, predicateHints = ["group = 'b'"])
len(files.add_files)

# COMMAND ----------

files.add_files[1].url

# COMMAND ----------

import fsspec
from urllib.parse import quote, urlparse

action = files.add_files[1]
print(action)

# COMMAND ----------

url = urlparse(action.url)
print(url)

# COMMAND ----------

protocol = url.scheme
print(protocol)

# COMMAND ----------

filesystem = fsspec.filesystem(protocol)
print(filesystem)

# COMMAND ----------

[x.url for x in files.add_files]

# COMMAND ----------

from pyarrow.dataset import dataset
print(action.url)
pa_dataset = dataset(source= [x.url for x in files.add_files], format="parquet", filesystem=filesystem)
print(pa_dataset)

print(pa_dataset.files)

# COMMAND ----------

pa_dataset.to_table().to_pandas()


# COMMAND ----------

result = ds.reader.DeltaSharingReader(table=table, rest_client=client)\
  .predicateHints(["date >= '2021-01-01'", "date <= '2021-01-31'", "group = 'b'"])\
  .to_pandas()

# COMMAND ----------

result

# COMMAND ----------


