import pandas as pd
from sqlalchemy import create_engine
import ibm_db

# Connect to DB2
dsn_hostname = "2f3279a5-73d1-4859-88f0-a6c3e6b4b907.c3n41cmd0nqnrk39u98g.databases.appdomain.cloud"
dsn_uid = "prg79418"        
dsn_pwd = "QOKWifsXGuPcg5Li"      
dsn_port = "30756"                
dsn_database = "bludb"            
dsn_driver = "{IBM DB2 ODBC DRIVER}"       
dsn_protocol = "TCPIP"           
dsn_security = "SSL"              

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};"
).format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

insert_dim_category_table = '''
    MERGE INTO DimCategory
    USING ( VALUES (?, ?) ) AS SOURCE(id, name)
    ON DimCategory.category_id = SOURCE.id
        WHEN MATCHED THEN UPDATE SET DimCategory.category = SOURCE.name
        WHEN NOT MATCHED THEN INSERT VALUES (SOURCE.id, SOURCE.name)
'''

def transform(df_book_products):
    df_book_products['category'] = df_book_products['category'].map(lambda x : str(x or '').strip().capitalize())
    df_category = df_book_products[['category_id', 'category']]
    df_category = df_category.drop_duplicates()
    return df_category

def main():   
    ibm_conn = ibm_db.connect(dsn, "", "")

    alchemyEngine = create_engine('postgresql+psycopg2://airflow:airflow@project-db:5432/airflow', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    df_category = transform(pd.read_sql("SELECT * FROM staging.book_product_data ORDER BY id ASC", dbConnection, index_col="id"))
    category_list = list(df_category.itertuples(index=False, name=None))
    
    insert_cateogory_stmt = ibm_db.prepare(ibm_conn, insert_dim_category_table)
    cnt = 0
    for category in category_list:
        # print(category)
        cnt = cnt + 1
        print(f"\n{cnt} / {len(category_list)}:", category[0])
        try:
            ibm_db.execute(insert_cateogory_stmt, category)
        except: 
            print("Error occurred with ", category[0])
        
    ibm_db.close(ibm_conn)

if __name__ == '__main__':
    main()