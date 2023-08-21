import connection.connect as con

cur = con.connection()


# Truncate the STG_D_REGION_LU table before loading data
cur.execute("TRUNCATE TABLE STG.STG_D_REGION_LU")

# Create the stage named STG.REGION_STAGE
cur.execute ("CREATE OR REPLACE STAGE STG.REGION_STAGE")

# Load data from the CSV file REGION.csv into the staging table STG_D_REGION_LU
cur.execute(r"PUT file://C:\Users\kapur.mahatara\Desktop\BootCamp\ETL_TEST\csv\REGION.csv @STG.REGION_STAGE")
cur.execute("COPY INTO STG.STG_D_REGION_LU FROM @STG.REGION_STAGE file_format = (type = csv field_delimiter = ',' skip_header = 1)")



# Truncate the TMP_D_REGION_LU table before inserting data
cur.execute("TRUNCATE TABLE BHATBHATENI_DB.TMP.TMP_D_REGION_LU")

# Inserting date from stg table to tmp
insert_data_query = """
INSERT INTO BHATBHATENI_DB.TMP.TMP_D_REGION_LU
SELECT *
FROM BHATBHATENI_DB.STG.STG_D_REGION_LU
"""

cur.execute(insert_data_query)


# Perform the upsert from TMP_D_REGION_LU to DWH_D_REGION_LU

upsert_query = f"""
MERGE INTO BHATBHATENI_DB.TGT.DWH_D_REGION_LU AS tgt
USING BHATBHATENI_DB.TMP.TMP_D_REGION_LU AS tmp
ON tgt.id = tmp.id
WHEN MATCHED THEN UPDATE SET
    tgt.region_desc = tmp.region_desc,
    tgt.country_id = tmp.country_id,
    tgt.UPDATE_TS = CASE WHEN tgt.REGION_desc != tmp.REGION_desc THEN CURRENT_TIMESTAMP() ELSE tgt.UPDATE_TS END,
    tgt.ACTIVE_RECORD = TRUE
WHEN NOT MATCHED THEN 
    INSERT (id, country_id, region_desc, INSERT_TS, UPDATE_TS, ACTIVE_RECORD) 
    VALUES (tmp.id, tmp.country_id, tmp.region_desc, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);
"""
 

update_query = f"""
UPDATE BHATBHATENI_DB.TGT.DWH_D_REGION_LU
SET UPDATE_TS = CURRENT_TIMESTAMP(), ACTIVE_RECORD = FALSE
WHERE id NOT IN (SELECT id FROM BHATBHATENI_DB.TMP.TMP_D_REGION_LU);
"""

 
cur.execute(upsert_query)
cur.execute(update_query)



# Commit the changes and close the cursor
cur.connection.commit()
cur.close()



