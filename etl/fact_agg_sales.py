import connection.connect as con

cur = con.connection()

# SQL query to truncate the target table
truncate_query = "TRUNCATE TABLE BHATBHATENI_DB.TGT.DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T"


cur.execute(truncate_query)


upsert_query = f"""INSERT INTO BHATBHATENI_DB.TGT.DWH_F_BHATBHATENI_AGG_SLS_PLC_MONTH_T 
                SELECT ID, STORE_ID, PRODUCT_ID, CUSTOMER_ID, MONTHNAME(TRANSACTION_TIME) Month_Name, SUM(QUANTITY), SUM(AMOUNT), SUM(DISCOUNT), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                FROM BHATBHATENI_DB.TMP.TMP_D_SALES_LU
                GROUP BY ID, STORE_ID, PRODUCT_ID, CUSTOMER_ID, Month_Name
                ORDER BY ID;
                """
cur.execute(upsert_query)


# Commit the changes and close the cursor
cur.connection.commit()
cur.close()
