# Lab - Spark Pool - Load data

from pyspark.sql import SparkSession
from pyspark.sql.types import *

account_name = "dataforbigdata345"
container_name = "data"
relative_path = "logdata"
adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path)

spark.conf.set("fs.azure.account.auth.type.%s.dfs.core.windows.net" %account_name, "SharedKey")
spark.conf.set("fs.azure.account.key.%s.dfs.core.windows.net" %account_name ,"qUy0gj0oOWoGiJicydrAW4hRiOwGgBb6nmVVGqoTmmPjjjAvsLwJ6j69tP4g/qHNztNaKTDpflOgjT1UOWAy1A==")

df1 = spark.read.option('header', 'true') \
                .option('delimiter', ',') \
                .csv(adls_path + '/Log.csv')

display(df1)

# The Spark groupBy function is used to collect identical data and segregate them into groups
# Then you can perform aggregation on the grouped data

from pyspark.sql.functions import *

# .agg is a method that can be used to perform aggregation based on a column of data
# .orderBy helps to order by a particular column
newdf=(df1.groupBy("Operationname")
     .agg(count("Correlationid").alias("Total operations"))
     .orderBy(col("Total operations").desc()))

display(newdf)

df1.createOrReplaceTempView('logfromazure')
%%sql
select * from logfromazure limit 3;