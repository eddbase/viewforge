CREATE CATALOG file_warehouse (
  TYPE = 'file',
  PATH = '/tmp/file_warehouse/'
);


CREATE CATALOG local_iceberg (
  TYPE = 'iceberg',
  WAREHOUSE = '/tmp/iceberg_warehouse'
);


CREATE CATALOG pg_catalog (
  TYPE = 'database',
  URL = 'jdbc:postgresql://localhost:5432/tpch',
  DRIVER = 'org.postgresql.Driver',
  USERNAME = 'postgres',
  PASSWORD = 'zxcv'
);


CREATE STREAM IcebergSource FROM local_iceberg.db.sample_iceberg_table;


CREATE STREAM customerStream FROM file_warehouse.`customer.csv` (
  format = 'csv',
  header = 'true',
  inferSchema = 'true'
);


CREATE VIEW orderCustomerView (
                           TARGET_LAG = '3 minute'
    ) AS
SELECT `O`.`o_orderkey`, `O`.`o_orderdate`, `O`.`o_shippriority`
FROM pg_catalog.tpch.orders AS `O` JOIN `customerStream` AS `C`
ON `O`.`o_custkey` = `C`.`c_custkey`;


CREATE VIEW joinResultView(
                               TARGET_LAG = '5 minute',
                               SINK = 'pg_catalog'
    ) AS
SELECT `O`.`o_orderkey`
FROM `orderCustomerView` AS `O` JOIN `IcebergSource` AS `I`;

