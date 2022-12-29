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

insert_fact_book_product_table = '''
    INSERT INTO FactBookProduct(product_id, category_id, sku, image_url, quantity_sold, price, original_price, discount, discount_rate) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

def transform(df_book_products):
    df_book_products['publication_date'] = pd.to_datetime(df_book_products.publication_date)
    df_book_products['publication_date'] = df_book_products['publication_date'].dt.strftime("%m-%d-%Y")
    df_book_products['publication_date'] = df_book_products['publication_date'].map(lambda x : None if x != x else x)
    df_fact_book_product = df_book_products[['product_id', 'category_id', 'sku', 'image_url', 'quantity_sold', 'price', 'original_price', 'discount', 'discount_rate']]    
    return df_fact_book_product

def main():   
    ibm_conn = ibm_db.connect(dsn, "", "")

    alchemyEngine = create_engine('postgresql+psycopg2://airflow:airflow@project-db:5432/airflow', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    df_fact_book_product = transform(pd.read_sql("SELECT * FROM staging.book_product_data ORDER BY id ASC", dbConnection, index_col="id"))
    book_product_list = list(df_fact_book_product.itertuples(index=False, name=None))
    
    insert_book_product_stmt = ibm_db.prepare(ibm_conn, insert_fact_book_product_table)
    cnt = 0
    for book_product in book_product_list:
        # print(book_product)
        cnt = cnt + 1
        print(f"\n{cnt} / {len(book_product_list)}:", book_product[0], book_product[1])
        try:
            ibm_db.execute(insert_book_product_stmt, book_product)
        except: 
            print("Error occurred with ", book_product[0])
    
    print("Done loading fact book product table!")
    ibm_db.close(ibm_conn)

if __name__ == '__main__':
    main()