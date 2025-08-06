CREATE CATALOG my_nessie (
  TYPE = 'iceberg',
  SUB_TYPE = 'nessie',
  URI = 'nessie_url',
  WAREHOUSE = 's3a://warehouse'
);

       -- 1. Define a catalog that points to your local Iceberg warehouse.
-- This uses the default "hadoop" sub_type.
CREATE CATALOG local_iceberg (
  TYPE = 'iceberg',
  WAREHOUSE = '/tmp/iceberg_warehouse'
);

-- 2. Create a source that points to the table you just created.
-- The table name must include the database/schema part.
CREATE STREAM IcebergSource FROM local_iceberg.`db.sample_iceberg_table`;

-- 3. Create a simple view to read from the Iceberg table.
-- This view will be written to a final sink table in PostgreSQL.
CREATE VIEW TestIcebergView (
                             TARGET_LAG = '5 minute',
                             SINK = 'pg_catalog'
    )
AS
SELECT
    category,
    value
FROM
    IcebergSource;

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

