import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import requests
import time
import random


url = 'https://tiki.vn/api/v2/reviews'

cookies = {
    'TOKENS': '{%22access_token%22:%22PKs3zEjMIJFvmobqUxaCfyw8npOh6HdR%22%2C%22expires_in%22:157680000%2C%22expires_at%22:1829290726320%2C%22guest_token%22:%22PKs3zEjMIJFvmobqUxaCfyw8npOh6HdR%22}',
    'amp_99d374': 'gjGLPDjQql1g5jXQD1O6hD...1gkpuvus5.1gkpv016r.15.1e.2j',
    '_gcl_au': '1.1.598139170.1671610738',
    '_trackity': '53118beb-c362-c41f-4337-3b5e803cc724',
    '_ga_GSD4ETCY1D': 'GS1.1.1671610738.1.1.1671613647.53.0.0',
    '_ga': 'GA1.1.999940323.1671610738',
    '_gali': '__next', 
    '__uidac': '2db3d4b93377bae94130e308f8961eaf',
    '__iid': '749',
    '__su': '0',
    '_gid': 'GA1.2.761225788.1671610739',
    '_hjFirstSeen': '1',
    '_hjAbsoluteSessionInProgress': '0',
    '_hjIncludedInSessionSample': '0',
    '_hjSession_522327': 'eyJpZCI6ImY5MGZkYmJiLThmN2ItNDA2NS05NjQ3LWVjZjVlNmVhMTU0NyIsImNyZWF0ZWQiOjE2NzE2MTA3Mzg3OTYsImluU2FtcGxlIjpmYWxzZX0',
    '_hjSessionUser_522327': 'eyJpZCI6IjQzZjQzOGE4LWEyM2ItNTI1Zi04YTQxLThmNTdjOGRiYTY5YyIsImNyZWF0ZWQiOjE2NzE2MTA3Mzg2MjcsImV4aXN0aW5nIjp0cnVlfQ==', 
    'tiki_client_id': '999940323.1671610738',
    'TKSESSID': 'f00181ef1e9098c6679f840c4409d79e',
    'TIKI_RECOMMENDATION': '6b752098d7d178314b06a24a81f83223',
    'cto_bundle': '2z9IqV83UmNIdE9lWG5rQ3ZrdDZNT1dCUWQ2d3R6NnlvT1JwTTN5R3JJJTJCT1Q2QnpwRyUyRkRwNzVWZmdnU2VQRHdzTUJaSSUyRk83a1R4VHluU093VkRTM0pCeHc2cnVGbjQ5SkpQbWhNRm4lMkI2VHBCTURXY1VnZCUyQkl1RlpmZGwzQk9iMWhrdVo3QXROWExyalZUSTJPeXM3b1RlelBBJTNEJTNE',
    'TIKI_RECENTLYVIEWED': '58259141',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Whale/3.18.154.7 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;en-US,en;q=0.9,ko;q=0.8',
    'x-guest-token': 'PKs3zEjMIJFvmobqUxaCfyw8npOh6HdR',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = {
    'product_id': '58259141',
    'sort': 'score|desc,id|desc,stars|all',
    'page': '1',
    'limit': '10',
    'include': 'comments,contribute_info,attribute_vote_summary'
}

def parse_reviews(json, pid):
    product_id = pid
    
    rating_average = json.get('rating_average')
    reviews_count = json.get('reviews_count')
    
    count_1_star = json.get('stars').get('1').get('count')
    percent_1_star  = json.get('stars').get('1').get('percent')
    
    count_2_star = json.get('stars').get('2').get('count')
    percent_2_star  = json.get('stars').get('2').get('percent')

    count_3_star = json.get('stars').get('3').get('count')
    percent_3_star  = json.get('stars').get('3').get('percent')

    count_4_star = json.get('stars').get('4').get('count')
    percent_4_star  = json.get('stars').get('4').get('percent')

    count_5_star = json.get('stars').get('5').get('count')
    percent_5_star  = json.get('stars').get('5').get('percent')
    
    values = (product_id, rating_average, reviews_count, count_1_star, percent_1_star, count_2_star, percent_2_star, count_3_star, percent_3_star, count_4_star, percent_4_star, count_5_star, percent_5_star)
    
    return values


insert_staging_table = '''
    INSERT INTO staging.book_product_review VALUES (
        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
'''

def main():
    alchemyEngine = create_engine('postgresql+psycopg2://airflow:airflow@project-db:5432/airflow', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    conn = psycopg2.connect(database = "airflow", user = "airflow", password = "airflow", host = "project-db", port = "5432")
    cur = conn.cursor()
    
    df_book_product_id = pd.read_sql("SELECT * FROM staging.book_product_id", dbConnection)
    product_id_list = df_book_product_id.product_id.to_list()
    
    cnt = 0
    for pid in product_id_list:
        cnt = cnt + 1
        if cnt == 100: break
        params['product_id'] = pid
        print('\nCrawl reviews for product {}'.format(pid))
        response = requests.get(url=url, headers=headers, params=params)
        if response.status_code == 200:
            values = parse_reviews(response.json(), pid)
            cur.execute(insert_staging_table, values)

    conn.commit()

    cur.close()
    conn.close()

if __name__ == '__main__':
    main() 