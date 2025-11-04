-------------------------
-- DimCompany
-------------------------
IF NOT EXISTS (
    SELECT *
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'DimCompany'
      AND s.name = 'gold'
)
CREATE EXTERNAL TABLE gold.DimCompany
WITH
(
    LOCATION    = 'DimCompany',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet
)
AS
SELECT
    T.*,ROW_NUMBER() OVER (ORDER BY company_id) AS DimCompanyKey
FROM (
    SELECT DISTINCT
        company_id,
        company_name,
        website,
        country,
        country_iso2,
        industry,
        parent_id
    FROM silver.silverTable
) T;

-------------------------
-- DimCertificate
-------------------------
IF NOT EXISTS (
    SELECT *
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'DimCertificate'
      AND s.name = 'gold'
)
CREATE EXTERNAL TABLE gold.DimCertificate
WITH
(
    LOCATION    = 'DimCertificate',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet
)
AS
SELECT
    T.*,ROW_NUMBER() OVER (ORDER BY certificate_id) AS DimCertificateKey,
    DATEDIFF(DAY, valid_from, valid_to) AS validity_days
FROM (
    SELECT DISTINCT
        certificate_id,
        standard_name,
        issuing_body,
        valid_from,
        valid_to,
        active_status
    FROM silver.silverTable
) T;

