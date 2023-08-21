import connection.connect as con

cur = con.connection()

# Truncate the STG_D_SUBCATEGORY_LU table before loading data
cur.execute("TRUNCATE TABLE STG.STG_D_SUBCATEGORY_LU")

# Create the stage named STG.SUBCATEGORY_STAGE
cur.execute ("CREATE OR REPLACE STAGE STG.SUBCATEGORY_STAGE")


# Load data from the CSV file SUBCATEGORY.csv into the staging table STG_D_SUBCATEGORY_LU
cur.execute(r"PUT file://C:\Users\kapur.mahatara\Desktop\BootCamp\ETL_TEST\csv\SUBCATEGORY.csv @STG.SUBCATEGORY_STAGE")
cur.execute("COPY INTO STG.STG_D_SUBCATEGORY_LU FROM @STG.SUBCATEGORY_STAGE file_format = (type = csv field_delimiter = ',' skip_header = 1)")


# Truncate the TMP_D_SUBCATEGORY_LU table before inserting data
cur.execute("TRUNCATE TABLE BHATBHATENI_DB.TMP.TMP_D_SUBCATEGORY_LU")


# Inserting data from stg to tmp
insert_data_query = """
INSERT INTO BHATBHATENI_DB.TMP.TMP_D_SUBCATEGORY_LU
SELECT *
FROM BHATBHATENI_DB.STG.STG_D_SUBCATEGORY_LU
"""

cur.execute(insert_data_query)


# Perform the upsert from TMP_D_SUBCATEGORY_LU to DWH_D_SUBCATEGORY_LU



upsert_query = f"""
MERGE INTO BHATBHATENI_DB.TGT.DWH_D_SUBCATEGORY_LU AS tgt
USING BHATBHATENI_DB.TMP.TMP_D_SUBCATEGORY_LU AS tmp
ON tgt.id = tmp.id
WHEN MATCHED THEN UPDATE SET
    tgt.subcategory_desc = tmp.subcategory_desc,
    tgt.category_id = tmp.category_id,
    tgt.UPDATE_TS = CASE WHEN tgt.subcategory_desc != tmp.subcategory_desc THEN CURRENT_TIMESTAMP() ELSE tgt.UPDATE_TS END,
    tgt.ACTIVE_RECORD = TRUE
WHEN NOT MATCHED THEN 
    INSERT (id, subcategory_desc, category_id, INSERT_TS, UPDATE_TS, ACTIVE_RECORD) 
    VALUES (tmp.id, tmp.subcategory_desc, tmp.category_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);
"""
 

update_query = f"""
UPDATE BHATBHATENI_DB.TGT.DWH_D_SUBCATEGORY_LU
SET UPDATE_TS = CURRENT_TIMESTAMP(), ACTIVE_RECORD = FALSE
WHERE id NOT IN (SELECT id FROM BHATBHATENI_DB.TMP.TMP_D_SUBCATEGORY_LU);
"""

 
cur.execute(upsert_query)
cur.execute(update_query)


# Commit the changes and close the cursor
cur.connection.commit()
cur.close()


