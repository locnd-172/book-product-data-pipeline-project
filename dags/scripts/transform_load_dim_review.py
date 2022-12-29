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

insert_dim_review_table = '''
    INSERT INTO DimReview(product_id, rating_average, reviews_count, count_1_star, percent_1_star, count_2_star, percent_2_star, count_3_star, percent_3_star, count_4_star, percent_4_star, count_5_star, percent_5_star) 
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

def transform(df_book_reviews):
    df_book_reviews['percent_1_star'] = df_book_reviews['percent_1_star'].map(lambda x : float(x) / 100)
    df_book_reviews['percent_2_star'] = df_book_reviews['percent_2_star'].map(lambda x : float(x) / 100)
    df_book_reviews['percent_3_star'] = df_book_reviews['percent_3_star'].map(lambda x : float(x) / 100)
    df_book_reviews['percent_4_star'] = df_book_reviews['percent_4_star'].map(lambda x : float(x) / 100)
    df_book_reviews['percent_5_star'] = df_book_reviews['percent_5_star'].map(lambda x : float(x) / 100)
    
    df_review = df_book_reviews[['product_id', 'rating_average', 'reviews_count', 'count_1_star', 'percent_1_star', 'count_2_star', 'percent_2_star', 'count_3_star', 'percent_3_star', 'count_4_star', 'percent_4_star', 'count_5_star', 'percent_5_star']]
    
    return df_review

def main():   
    ibm_conn = ibm_db.connect(dsn, "", "")

    alchemyEngine = create_engine('postgresql+psycopg2://airflow:airflow@project-db:5432/airflow', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    df_review = transform(pd.read_sql("SELECT * FROM staging.book_product_review ORDER BY id ASC", dbConnection, index_col="id"))
    review_list = list(df_review.itertuples(index=False, name=None))
    
    insert_review_stmt = ibm_db.prepare(ibm_conn, insert_dim_review_table)
    cnt = 0
    for review in review_list:
        # print(review)
        cnt = cnt + 1
        print(f"\n{cnt} / {len(review_list)}:", review[0], review[1])
        try: 
            ibm_db.execute(insert_review_stmt, review)
        except: 
            print("Error occurred with ", review[0])
        

    ibm_db.close(ibm_conn)

if __name__ == '__main__':
    main()