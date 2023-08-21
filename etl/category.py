import connection.connect as con

cur = con.connection()
 

# Truncate the STG_D_CATEGORY_LU table before loading data
cur.execute("TRUNCATE TABLE STG.STG_D_CATEGORY_LU")

# Create the stage named STG.CATEGORY_STAGE
cur.execute ("CREATE OR REPLACE STAGE STG.CATEGORY_STAGE")


# Load data from the CSV file CATEGORY.csv into the staging table STG_D_CATEGORY_LU
cur.execute(r"PUT file://C:\Users\kapur.mahatara\Desktop\BootCamp\ETL_TEST\csv\CATEGORY.csv @STG.CATEGORY_STAGE")
cur.execute("COPY INTO STG.STG_D_CATEGORY_LU FROM @STG.CATEGORY_STAGE file_format = (type = csv field_delimiter = ',' skip_header = 1)")


# Truncate the TMP_D_CATEGORY_LU table before inserting data
cur.execute("TRUNCATE TABLE BHATBHATENI_DB.TMP.TMP_D_CATEGORY_LU")


# Inserting data from stg to tmp
insert_data_query = """
INSERT INTO BHATBHATENI_DB.TMP.TMP_D_CATEGORY_LU
SELECT *
FROM BHATBHATENI_DB.STG.STG_D_CATEGORY_LU
"""

cur.execute(insert_data_query)




# Perform the upsert from TMP_D_CATEGORY_LU to DWH_D_CATEGORY_LU

upsert_query = """
MERGE INTO BHATBHATENI_DB.TGT.DWH_D_CATEGORY_LU AS tgt
USING BHATBHATENI_DB.TMP.TMP_D_CATEGORY_LU AS tmp
ON tgt.id = tmp.id
WHEN MATCHED THEN UPDATE SET
    tgt.category_desc = tmp.category_desc,
    tgt.UPDATE_TS = CASE WHEN tgt.category_desc != tmp.category_desc THEN CURRENT_TIMESTAMP() ELSE tgt.UPDATE_TS END,
    tgt.ACTIVE_RECORD = TRUE
WHEN NOT MATCHED THEN INSERT (
    id, category_desc, INSERT_TS, UPDATE_TS, ACTIVE_RECORD
) VALUES (
    tmp.id, tmp.category_desc, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE
);
"""
 

update_query = """
UPDATE BHATBHATENI_DB.TGT.DWH_D_CATEGORY_LU
SET UPDATE_TS = CURRENT_TIMESTAMP(), ACTIVE_RECORD = FALSE
WHERE id NOT IN (SELECT id FROM BHATBHATENI_DB.TMP.TMP_D_CATEGORY_LU);
"""

 
cur.execute(upsert_query)
cur.execute(update_query)


# Commit the changes and close the cursor
cur.connection.commit()
cur.close()




