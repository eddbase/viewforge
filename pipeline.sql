CREATE CATALOG my_nessie (
  TYPE = 'iceberg',
  SUB_TYPE = 'nessie',
  URI = 'nessie_url',
  WAREHOUSE = 's3a://warehouse'
);

CREATE CATALOG pg_catalog (
  TYPE = 'database',
  URL = 'jdbc:postgresql://localhost:5432/tpch',
  DRIVER = 'org.postgresql.Driver',
  USERNAME = 'postgres',
  PASSWORD = 'zxcv'
);

CREATE STREAM OrderStream FROM pg_catalog.Orders;

CREATE STREAM LineitemStream FROM pg_catalog.Lineitem(
  CDC_TIMESTAMP = 'last_update_date'
);

CREATE VIEW OrderCustomer (
  TARGET_LAG = '1 minute'
)
AS
  SELECT O.o_orderkey, O.o_orderdate, O.o_shippriority, COUNT(*) AS cnt
    FROM OrderStream AS O JOIN pg_catalog.tpch.Customer AS C
      ON O.o_custkey = C.c_custkey
   WHERE C.c_mktsegment = 'BUILDING'
     AND O.o_orderdate < DATE('1995-03-15')
GROUP BY O.o_orderkey, O.o_orderdate, O.o_shippriority;

CREATE VIEW TPCH3 (
  TARGET_LAG = '2 minute',
  SINK = 'pg_catalog'
)
AS 
  SELECT OC.o_orderkey, OC.o_orderdate, OC.o_shippriority, 
         SUM(cnt * L.l_extendedprice * (1 - L.l_discount)) AS revenue
    FROM LineitemStream AS L JOIN OrderCustomer AS OC
      ON L.l_orderkey = OC.o_orderkey
   WHERE L.l_shipdate > DATE('1995-03-15')
GROUP BY OC.o_orderkey, OC.o_orderdate, OC.o_shippriority;

