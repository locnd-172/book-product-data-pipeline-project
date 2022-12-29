import psycopg2
from sqlalchemy import create_engine
import re
import pandas as pd
import requests
import time
import random
import json

url = 'https://tiki.vn/api/v2/products/{}'

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

params = (
    ('platform', 'web'),
    ('spid', 187266106)
)

insert_staging_table = '''
    INSERT INTO staging.book_product_data VALUES (
        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON CONFLICT ON CONSTRAINT product_id_unique DO NOTHING;
'''

def get_attr_value(attributes, field):
    ls = [attr.get('value') for attr in attributes if attr.get('code') == field]
    return ls[0] if len(ls) > 0 else None

def parser_product(json):
    product_id = json.get('id')
    sku = json.get('sku')
    name = json.get('name')
    
    price = json.get('price')
    original_price = json.get('original_price')    
    discount = json.get('discount')
    discount_rate = json.get('discount_rate')
    
    image_url = json.get('images')[0].get('base_url')
    
    author = json.get('authors')
    author = author[0].get('name') if author != None else None
    
    quantity_sold = json.get('quantity_sold').get('value') if json.get('quantity_sold') != None else 0
    
    attributes = json.get('specifications')[0].get('attributes')
    
    publisher = get_attr_value(attributes, 'publisher_vn')
    manufacturer = get_attr_value(attributes, 'manufacturer')
    
    pages = get_attr_value(attributes, 'number_of_page')
    number_of_pages = int(pages) if pages else 0
    translator = get_attr_value(attributes, 'dich_gia')
    publication_date = get_attr_value(attributes, 'publication_date')
    book_cover = get_attr_value(attributes, 'book_cover')
    
    tmp_dim = get_attr_value(attributes, 'dimensions')
    width, height = 0, 0
    if tmp_dim: 
        dimensions = re.findall(r'\d+\.\d+|\d+', tmp_dim)
        if len(dimensions) == 2:
            width = float(dimensions[0])
            height = float(dimensions[1])
    
    category = json.get('breadcrumbs')[2].get('name')
    category_id = json.get('breadcrumbs')[2].get('category_id')

    values = (product_id, name, sku, price, original_price, discount, discount_rate, image_url, author, quantity_sold, publisher, manufacturer, number_of_pages, translator, publication_date, book_cover, width, height, category, category_id)
    return values


def main():
    alchemyEngine = create_engine('postgresql+psycopg2://airflow:airflow@project-db:5432/airflow', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    conn = psycopg2.connect(database = "airflow", user = "airflow", password = "airflow", host = "project-db", port = "5432")
    cur = conn.cursor()
    
    df_book_product_id = pd.read_sql("SELECT * FROM staging.book_product_id", dbConnection)
    product_id_list = df_book_product_id.product_id.to_list()
    
    cnt = 0
    count_product = 0
    for pid in product_id_list:
        cnt = cnt + 1
        print(f"\n{cnt} / {len(product_id_list)}: ")
        response = requests.get(url=url.format(pid), headers=headers, params=params, cookies=cookies)
        print('Crawl data for product {}'.format(pid))
        if response.status_code == 200:
            try:
                values = parser_product(response.json())
                cur.execute(insert_staging_table, values)
                count_product = count_product + 1
                print('Success!!!')
            except:
                print("Errors occur!!!")
       
        time.sleep(random.randrange(1, 2)) 

    print(f"Crawled: {count_product} products' data")

    conn.commit()

    cur.close()
    conn.close()
    
    
if __name__ == '__main__':
    main() 