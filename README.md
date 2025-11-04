# CommonShare Technical Assessment

## Logic
**Medallion Architecture:** 
- Bronze: Raw CSV ingestion (companies, certifications, company_certificates)
- Silver: Data cleaning (null filtering, ISO2 normalization, active_status derivation)
- Gold: Star schema (DimCompany, DimCertificate, FactCompanyCertificate)

**Star Schema:**
- Fact: FactCompanyCertificate 
- Dims: DimCompany , DimCertificate 
- Keys: Surrogate keys (DimCompanyKey, DimCertificateKey)
- Metric: validity_duration_days = DATEDIFF(valid_from, valid_to)

**Data Quality:**
- Null filtering: company_name, certificate_id
- Country normalization: USA→US, UK→UK, etc.
- Active status: valid_to ≥ TODAY()
- Referential integrity: INNER JOINs in Gold layer

## Trade-offs
| Decision | Why | Trade-off |
|----------|-----|-----------|
| **Synapse SQL** | Cost-efficient, serverless | Less flexible than PySpark |
| **External Tables** | Direct ADLS access, no copy | Potential latency vs. cached |
| **Parquet** | Compression (75%), columnar | Not human-readable |
| **Surrogate Keys** | Prevents collisions | More complexity |
| **INNER JOINs** | Referential integrity | Loses orphaned records |

## Next Steps
1. **Incremental loads:** Implement CDC for daily deltas
2. **SCD Type 2:** Track parent_id changes over time
3. **Automation:** Deploy via Azure Data Factory with error handling
4. **Governance:** Add Purview cataloging and data quality monitoring
5. **Scale:** Migrate to Databricks for 100GB+ datasets with PySpark

## ERP Integration Risk
**Problem:** Facility records linked to non-existent parent companies  
**Solution:** Validation query detects orphaned/mismatched records  
**Automation:** Daily pipeline check with alert on failures

---
GitHub: https://github.com/ziatily2/commonshare
