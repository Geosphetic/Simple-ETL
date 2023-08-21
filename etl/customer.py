import connection.connect as con

cur = con.connection()

# Truncate the STG_D_CUSTOMER_LU table before loading data
cur.execute("TRUNCATE TABLE STG.STG_D_CUSTOMER_LU")

# create internal stage
cur.execute("CREATE OR REPLACE STAGE STG.CUSTOMER_STAGE")


#Put and Copy INTO the staging tables using the csv file from source tables database
cur.execute(r"PUT file://C:\Users\kapur.mahatara\Desktop\BootCamp\ETL_TEST\csv\CUSTOMER.csv @STG.CUSTOMER_STAGE")
cur.execute("COPY INTO BHATBHATENI_DB.STG.STG_D_CUSTOMER_LU FROM @STG.CUSTOMER_STAGE file_format = (type = csv field_delimiter = ',' skip_header = 1)")


# Truncate the TMP_D_CUSTOMER_LU table before inserting data
cur.execute("TRUNCATE TABLE BHATBHATENI_DB.TMP.TMP_D_CUSTOMER_LU")



# Inserting the Values from Staging Tables into Temporary Tables
insert_data_query = """
INSERT INTO BHATBHATENI_DB.TMP.TMP_D_CUSTOMER_LU
SELECT DISTINCT *
FROM BHATBHATENI_DB.STG.STG_D_CUSTOMER_LU
"""
cur.execute(insert_data_query)



# Perform the upsert from TMP_D_CUSTOMER_LU to DWH_D_CUSTOMER_LU


upsert_query = f"""
MERGE INTO BHATBHATENI_DB.TGT.DWH_D_CUSTOMER_LU AS tgt
USING BHATBHATENI_DB.TMP.TMP_D_CUSTOMER_LU AS tmp
ON tgt.id = tmp.id
WHEN MATCHED THEN UPDATE SET
    tgt.customer_first_name = tmp.customer_first_name,
    tgt.customer_middle_name = tmp.customer_middle_name,
    tgt.customer_last_name = tmp.customer_last_name,
    tgt.UPDATE_TS = CASE WHEN (tgt.customer_first_name != tmp.customer_first_name OR tgt.customer_middle_name != tmp.customer_middle_name OR tgt.customer_last_name != tmp.customer_last_name) THEN CURRENT_TIMESTAMP() ELSE tgt.UPDATE_TS END,
    tgt.ACTIVE_RECORD = TRUE
WHEN NOT MATCHED THEN INSERT (
    id, customer_first_name,customer_middle_name, customer_last_name, INSERT_TS, UPDATE_TS, ACTIVE_RECORD) 
    VALUES (tmp.id, tmp.customer_first_name, tmp.customer_middle_name, tmp.customer_last_name, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), TRUE);
"""
 

update_query = f"""
UPDATE BHATBHATENI_DB.TGT.DWH_D_CUSTOMER_LU
SET UPDATE_TS = CURRENT_TIMESTAMP(), ACTIVE_RECORD = FALSE
WHERE id NOT IN (SELECT id FROM BHATBHATENI_DB.TMP.TMP_D_CUSTOMER_LU);
"""

 
cur.execute(upsert_query)
cur.execute(update_query)



# Commit the changes and close the cursor
cur.connection.commit()
cur.close()


