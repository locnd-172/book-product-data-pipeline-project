import requests
import time
import random
import json
import psycopg2

url = 'https://tiki.vn/api/personalish/v1/blocks/listings'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Whale/3.18.154.7 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;en-US,en;q=0.9,ko;q=0.8',
    'Referer': 'https://tiki.vn/sach-truyen-tieng-viet/c316',
    'x-guest-token': 'PKs3zEjMIJFvmobqUxaCfyw8npOh6HdR',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = {
    'limit': '40',
    'aggregations': '1',
    'trackity_id': '53118beb-c362-c41f-4337-3b5e803cc724',
    'category': '316',
    'page': '1',
    'src': 'c316',
    'urlKey': 'sach-truyen-tieng-viet',
}

insert_staging_product_id_table = '''
    INSERT INTO staging.book_product_id VALUES (%s);
'''

def main():
    conn = psycopg2.connect(database = "airflow", user = "airflow", password = "airflow", host = "project-db", port = "5432")
    cur = conn.cursor()

    response = requests.get(url=url, headers=headers, params=params)
    res = response.json()

    last_page = res['paging']['last_page']
    product_id = []

    count_product = 0
    for i in range(1, last_page + 1):
        params['page'] = i
        response = requests.get(url=url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                for record in response.json().get('data'):
                    name = record.get('name')
                    if "combo" in name.lower(): continue # exclude the combo product
                    cur.execute(insert_staging_product_id_table, [record.get('id')])
                    count_product = count_product + 1
                print(f'\n\t{i} requests success!')
            except:
                print("Errors occur!!!")
        # time.sleep(random.randrange(2, 3))

    print(f"Last page: {last_page}")
    print(f"Crawled: {count_product} products' id")
        
    conn.commit()

    cur.close()
    conn.close()
    
if __name__ == '__main__':
    main() 