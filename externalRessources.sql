CREATE DATABASE CommonShareDW;

USE CommonShareDW;

-------------------------------
-- MASTER KEY (encryption)
-------------------------------
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Commonshare1!';


-------------------------------
-- DATABASE SCOPED CREDENTIAL
-------------------------------
CREATE DATABASE SCOPED CREDENTIAL ilyas_creds
WITH IDENTITY = 'Managed Identity';



--------------------------
-- EXTERNAL DATA SOURCES
--------------------------
CREATE EXTERNAL DATA SOURCE bronze_source
WITH
(
    LOCATION  = 'https://stcommonshareilyas.dfs.core.windows.net/bronze/',
    CREDENTIAL = ilyas_creds
);


CREATE EXTERNAL DATA SOURCE silver_source
WITH
(
    LOCATION  = 'https://stcommonshareilyas.dfs.core.windows.net/silver/',
    CREDENTIAL = ilyas_creds
);


CREATE EXTERNAL DATA SOURCE gold_source
WITH
(
    LOCATION  = 'https://stcommonshareilyas.dfs.core.windows.net/gold/',
    CREDENTIAL = ilyas_creds
);


--------------------------
-- EXTERNAL FILE FORMATS
--------------------------

CREATE EXTERNAL FILE FORMAT csv_with_header
WITH
(
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS
    (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW        = 2
    )
);



CREATE EXTERNAL FILE FORMAT parquet
WITH
(
    FORMAT_TYPE      = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);