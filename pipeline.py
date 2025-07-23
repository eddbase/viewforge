from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
  .appName("SparkExample") \
  .config("spark.jars", "/Users/mnikolic/repo/projects/ViewForge/lib/postgresql-42.7.5.jar") \
  .getOrCreate()

# JDBC connection parameters for pg_catalog
pg_catalog_url1 = "jdbc:postgresql://localhost:5432/postgres"
pg_catalog_properties1 = {
  "DRIVER" : "org.postgresql.Driver"
}

df_orderss1 = spark.read.jdbc( \
  url=pg_catalog_url1, \
  properties=pg_catalog_properties1, \
  table="Orderss" \
)
df_orderss1.createOrReplaceTempView("orderss1")

df_lineitemm1 = spark.read.jdbc( \
  url=pg_catalog_url1, \
  properties=pg_catalog_properties1, \
  table="Lineitemm" \
)
df_lineitemm1.createOrReplaceTempView("lineitemm1")

df_tpch_customer1 = spark.read.jdbc( \
  url=pg_catalog_url1, \
  properties=pg_catalog_properties1, \
  table="tpch.Customer" \
)
df_tpch_customer1.createOrReplaceTempView("tpch_customer1")

query_ordercustomer1 = "\
  SELECT `O`.`o_orderkey`, `O`.`o_orderdate`, `O`.`o_shippriority`, COUNT(*) AS `cnt` \
  FROM `orderss1` AS `O` \
  INNER JOIN `tpch_customer1` AS `C` ON `O`.`o_custkey` = `C`.`c_custkey` \
  WHERE `C`.`c_mktsegment` = 'BUILDING' AND `O`.`o_orderdate` < `DATE`('1995-03-15') \
  GROUP BY `O`.`o_orderkey`, `O`.`o_orderdate`, `O`.`o_shippriority` \
"

df_ordercustomer1 = spark.sql(query_ordercustomer1)
df_ordercustomer1.createOrReplaceTempView("ordercustomer1")

query_tpch31 = "\
  SELECT `OC`.`o_orderkey`, `OC`.`o_orderdate`, `OC`.`o_shippriority`, SUM(`cnt` * `L`.`l_extendedprice` * (1 - `L`.`l_discount`)) AS `revenue` \
  FROM `lineitemm1` AS `L` \
  INNER JOIN `ordercustomer1` AS `OC` ON `L`.`l_orderkey` = `OC`.`o_orderkey` \
  WHERE `L`.`l_shipdate` > `DATE`('1995-03-15') \
  GROUP BY `OC`.`o_orderkey`, `OC`.`o_orderdate`, `OC`.`o_shippriority` \
"

df_tpch31 = spark.sql(query_tpch31)
df_tpch31.createOrReplaceTempView("tpch31")

df_tpch31.write.jdbc( \
  url=pg_catalog_url1, \
  properties=pg_catalog_properties1, \
  table="TPCH3" \
)

