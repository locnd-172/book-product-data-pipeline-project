import pandas as pd
from sqlalchemy import create_engine
import psycopg2
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

insert_dim_book_table = '''
    INSERT INTO DimBook(product_id, name, author, publisher, manufacturer, number_of_pages, translator, publication_date, book_cover, width, height) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

def transform(df_book_products):
    df_book_products['number_of_pages'] = df_book_products['number_of_pages'].map(lambda x : int(str(x or '0')))
    df_book_products['discount_rate'] = df_book_products['discount_rate'].map(lambda x : float(x) / 100)
    df_book_products['author'] = df_book_products['author'].map(lambda x : str(x or '').strip().title())
    df_book_products['publisher'] = df_book_products['publisher'].map(lambda x : str(x or '').strip().title())
    df_book_products['manufacturer'] = df_book_products['manufacturer'].map(lambda x : str(x or '').strip().title())
    df_book_products['translator'] = df_book_products['translator'].map(lambda x : str(x or '').strip().title())
    df_book_products['book_cover'] = df_book_products['book_cover'].map(lambda x : str(x or '').strip().capitalize())

    df_book_products['publication_date'] = pd.to_datetime(df_book_products.publication_date)
    df_book_products['publication_date'] = df_book_products['publication_date'].dt.strftime("%m-%d-%Y")
    df_book_products['publication_date'] = df_book_products['publication_date'].map(lambda x : None if x != x else x)

    df_book = df_book_products[['product_id', 'name', 'author', 'publisher', 'manufacturer', 'number_of_pages', 'translator', 'publication_date', 'book_cover', 'width', 'height']]
    return df_book

def main():   
    ibm_conn = ibm_db.connect(dsn, "", "")

    postgres_conn = psycopg2.connect(database = "airflow", user = "airflow", password = "airflow", host = "project-db", port = "5432")
    postgres_cur = postgres_conn.cursor()

    alchemyEngine = create_engine('postgresql+psycopg2://airflow:airflow@project-db:5432/airflow', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    # df_book_products = pd.read_sql("SELECT * FROM staging.book_product_data ORDER BY id ASC", dbConnection, index_col="id")

    df_book = transform(pd.read_sql("SELECT * FROM staging.book_product_data ORDER BY id ASC", dbConnection, index_col="id"))
    book_list = list(df_book.itertuples(index=False, name=None))
    
    insert_book_stmt = ibm_db.prepare(ibm_conn, insert_dim_book_table)
    for book in book_list:
        # print(book)
        ibm_db.execute(insert_book_stmt, book)

    ibm_db.close(ibm_conn)

if __name__ == '__main__':
    main()