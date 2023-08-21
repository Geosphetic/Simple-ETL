import connection.connect as con

cur = con.connection()

# Truncate the STG_D_COUNTRY_LU table before loading data
cur.execute("TRUNCATE TABLE STG.STG_D_COUNTRY_LU")

# Create the stage named STG.COUNTRY_STAGE
cur.execute("CREATE OR REPLACE STAGE STG.COUNTRY_STAGE")

# Load data from the CSV file COUNTRY.csv into the staging table STG_D_COUNTRY_LU
cur.execute(r"PUT file://C:\Users\kapur.mahatara\Desktop\BootCamp\ETL_TEST\csv\COUNTRY.csv @STG.COUNTRY_STAGE")
cur.execute("COPY INTO STG.STG_D_COUNTRY_LU FROM @STG.COUNTRY_STAGE file_format = (type = csv field_delimiter = ',' skip_header = 1)")

# Truncate the TMP_D_COUNTRY_LU table before inserting data
cur.execute("TRUNCATE TABLE BHATBHATENI_DB.TMP.TMP_D_COUNTRY_LU")

# Insert data from STG_D_COUNTRY_LU to TMP_D_COUNTRY_LU
insert_data_query = """
INSERT INTO BHATBHATENI_DB.TMP.TMP_D_COUNTRY_LU
SELECT *
FROM STG.STG_D_COUNTRY_LU
"""
cur.execute(insert_data_query)


# Perform the upsert from STG_D_COUNTRY_LU to DWH_D_COUNTRY_LU

    

upsert_query = f"""
MERGE INTO BHATBHATENI_DB.TGT.DWH_D_COUNTRY_LU AS tgt
USING BHATBHATENI_DB.TMP.TMP_D_COUNTRY_LU AS tmp
ON tgt.id = tmp.id
WHEN MATCHED THEN UPDATE SET
    tgt.COUNTRY_desc = tmp.COUNTRY_desc,
    tgt.UPDATE_TS = CASE WHEN tgt.COUNTRY_desc != tmp.COUNTRY_desc THEN CURRENT_TIMESTAMP() ELSE tgt.UPDATE_TS END,
    tgt.ACTIVE_RECORD = TRUE
WHEN NOT MATCHED THEN 
    INSERT (id, COUNTRY_desc, INSERT_TS, UPDATE_TS, ACTIVE_RECORD) 
    VALUES (tmp.id, tmp.COUNTRY_desc, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);
"""
 

update_query = f"""
UPDATE BHATBHATENI_DB.TGT.DWH_D_COUNTRY_LU
SET UPDATE_TS = CURRENT_TIMESTAMP(), ACTIVE_RECORD = FALSE
WHERE id NOT IN (SELECT id FROM BHATBHATENI_DB.TMP.TMP_D_COUNTRY_LU);
"""

 
cur.execute(upsert_query)
cur.execute(update_query)


# Commit the changes and close the cursor
cur.connection.commit()
cur.close()


