import connection.connect as con

cur = con.connection()
 

# Truncate the STG_D_SALES_LU table before loading data
cur.execute("TRUNCATE TABLE STG.STG_D_SALES_LU")

# Create the stage named STG.SALES_STAGE
cur.execute ("CREATE OR REPLACE STAGE STG.SALES_STAGE")


# Load data from the CSV file SALES.csv into the staging table STG_D_SALES_LU
cur.execute(r"PUT file://C:\Users\kapur.mahatara\Desktop\BootCamp\ETL_TEST\csv\SALES.csv @STG.SALES_STAGE")
cur.execute("COPY INTO STG.STG_D_SALES_LU FROM @STG.SALES_STAGE file_format = (type = csv field_delimiter = ',' skip_header = 1)")


# Truncate the TMP_D_SALES_LU table before inserting data
cur.execute("TRUNCATE TABLE BHATBHATENI_DB.TMP.TMP_D_SALES_LU")

# Inserting data from stg to tmp
insert_data_query = """
INSERT INTO BHATBHATENI_DB.TMP.TMP_D_SALES_LU
SELECT *
FROM BHATBHATENI_DB.STG.STG_D_SALES_LU
"""

cur.execute(insert_data_query)



# Perform the upsert from TMP_D_SALES_LU to DWH_F_BHATBHATENI_SLS_TRXN_B


upsert_query = f"""
MERGE INTO BHATBHATENI_DB.TGT.DWH_F_BHATBHATENI_SLS_TRXN_B AS tgt
USING BHATBHATENI_DB.TMP.TMP_D_SALES_LU AS tmp
ON tgt.id = tmp.id
WHEN MATCHED THEN UPDATE SET
    tgt.store_id = tmp.store_id,
    tgt.customer_id = tmp.customer_id,
    tgt.product_id = tmp.product_id,
    tgt.day_transaction_time = DAY(tmp.transaction_time),
    tgt.quantity = tmp.quantity,
    tgt.amount = tmp.amount,
    tgt.discount = tmp.discount,
    tgt.UPDATE_TS = CASE WHEN tgt.amount != tmp.amount THEN CURRENT_TIMESTAMP() ELSE tgt.UPDATE_TS END
WHEN NOT MATCHED THEN 
    INSERT (id, store_id, product_id, customer_id, day_transaction_time, quantity, amount, discount, INSERT_TS, UPDATE_TS) 
    VALUES (tmp.id, tmp.store_id, tmp.product_id, tmp.customer_id, DAY(tmp.transaction_time), tmp.quantity, tmp.amount, tmp.discount, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
"""
 

update_query = f"""
UPDATE BHATBHATENI_DB.TGT.DWH_F_BHATBHATENI_SLS_TRXN_B
SET UPDATE_TS = CURRENT_TIMESTAMP()
WHERE id NOT IN (SELECT id FROM BHATBHATENI_DB.TMP.TMP_D_SALES_LU);
"""

 
cur.execute(upsert_query)
cur.execute(update_query)



# Commit the changes and close the cursor
cur.connection.commit()
cur.close()



