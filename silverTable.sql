IF NOT EXISTS ( SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = 'silverTable' AND s.name = 'silver')
CREATE EXTERNAL TABLE silver.silverTable
WITH
(
    LOCATION    = 'enrichedSilverData',   
    DATA_SOURCE = silver_source,
    FILE_FORMAT = parquet
)
AS
SELECT
    c.company_id,
    c.name              AS company_name,
    c.website,
    c.country,
    CASE 
        WHEN c.country IN ('United States', 'USA', 'US') THEN 'US'
        WHEN c.country = 'United Kingdom'                THEN 'UK'
        WHEN c.country = 'France'                        THEN 'FR'
        WHEN c.country = 'Germany'                       THEN 'DE'
        ELSE UPPER(LEFT(c.country, 2))
    END AS country_iso2,
    c.industry,
    c.parent_id,

    cert.certificate_id,
    cert.standard_name,
    cert.issuing_body,
    TRY_CAST(cert.valid_from AS DATE) AS valid_from,
    TRY_CAST(cert.valid_to   AS DATE) AS valid_to,

    CASE 
        WHEN TRY_CAST(cert.valid_to AS DATE) >= CAST(GETDATE() AS DATE) THEN 'True'
        ELSE 'False'
    END AS active_status,

    cc.facility_location,
    cc.scope,
    cc.status
FROM bronze.companies            AS c
JOIN bronze.company_certificates AS cc
    ON c.company_id      = cc.company_id
JOIN bronze.certifications       AS cert
    ON cc.certificate_id = cert.certificate_id   
WHERE
    c.name IS NOT NULL
    AND cert.certificate_id IS NOT NULL;



SELECT * FROM silver.silverTable;
