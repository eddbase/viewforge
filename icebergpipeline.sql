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

CREATE VIEW TestIcebergView (
                             TARGET_LAG = '5 minute',
                             SINK = 'pg_catalog'
    )
AS
SELECT
    `category`,
    `value`
FROM
    `IcebergSource`;