CREATE CATALOG file_warehouse (
  TYPE = 'file',
  PATH = '/tmp/file_warehouse/'
);


CREATE STREAM OrdersStream FROM file_warehouse.`orders.csv` (
  format = 'csv',
  header = 'true',
  inferSchema = 'true'
);

CREATE STREAM customerStream FROM file_warehouse.`customer.csv` (
  format = 'csv',
  header = 'true',
  inferSchema = 'true'
);

CREATE CATALOG pg_catalog (
  TYPE = 'database',
  URL = 'jdbc:postgresql://localhost:5432/tpch',
  DRIVER = 'org.postgresql.Driver',
  USERNAME = 'postgres',
  PASSWORD = 'zxcv'
);

CREATE VIEW orderCustomer (
                           TARGET_LAG = '5 minute',
                           SINK = 'pg_catalog'
    ) AS
SELECT `O`.`o_orderkey`, `O`.`o_orderdate`, `O`.`o_shippriority`, COUNT(*) AS `cnt`
FROM `OrdersStream` AS `O` JOIN `customerStream` AS `C`
                                ON `O`.`o_custkey` = `C`.`c_custkey`
WHERE `C`.`c_mktsegment` = 'BUILDING'
  AND `O`.`o_orderdate` < DATE('1995-03-15')
        GROUP BY `O`.`o_orderkey`, `O`.`o_orderdate`, `O`.`o_shippriority`;