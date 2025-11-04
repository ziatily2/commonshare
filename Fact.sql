IF NOT EXISTS (
    SELECT *
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'FactCompanyCertificate'
      AND s.name = 'gold'
)
CREATE EXTERNAL TABLE gold.FactCompanyCertificate
WITH
(
    LOCATION    = 'FactCompanyCertificate/',
    DATA_SOURCE = gold_source,
    FILE_FORMAT = parquet
)
AS
SELECT
    dc.DimCompanyKey,
    dcert.DimCertificateKey,
    s.facility_location,
    s.scope,
    s.status
FROM silver.silverTable s
JOIN gold.DimCompany     dc
    ON s.company_id     = dc.company_id
JOIN gold.DimCertificate dcert
    ON s.certificate_id = dcert.certificate_id;

SELECT * FROM gold.FactCompanyCertificate;
