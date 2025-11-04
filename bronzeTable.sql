-----------------------------
-- companies
-----------------------------
IF NOT EXISTS (
    SELECT *
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'companies'
      AND s.name = 'bronze'
)
CREATE EXTERNAL TABLE bronze.companies
(
    company_id   VARCHAR(50),
    name         VARCHAR(200),
    website      VARCHAR(500),
    country      VARCHAR(100),
    industry     VARCHAR(100),
    parent_id    VARCHAR(50) NULL
)
WITH
(
    LOCATION    = 'companies.csv',
    DATA_SOURCE = bronze_source,
    FILE_FORMAT = csv_with_header
);

-----------------------------
-- certifications
-----------------------------
IF NOT EXISTS (
    SELECT *
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'certifications'
      AND s.name = 'bronze'
)
CREATE EXTERNAL TABLE bronze.certifications
(
    certificate_id VARCHAR(50),
    standard_name  VARCHAR(200),
    issuing_body   VARCHAR(200),
    valid_from     VARCHAR(50),
    valid_to       VARCHAR(50)
)
WITH
(
    LOCATION    = 'certifications.csv',
    DATA_SOURCE = bronze_source,
    FILE_FORMAT = csv_with_header
);

-----------------------------
-- company_certificates
-----------------------------
IF NOT EXISTS (
    SELECT *
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'company_certificates'
      AND s.name = 'bronze'
)
CREATE EXTERNAL TABLE bronze.company_certificates
(
    company_id        VARCHAR(50),
    certificate_id    VARCHAR(50),
    facility_location VARCHAR(200),
    scope             VARCHAR(200),
    status            VARCHAR(50)
)
WITH
(
    LOCATION    = 'company_certificates.csv',
    DATA_SOURCE = bronze_source,
    FILE_FORMAT = csv_with_header
);



