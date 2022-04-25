import pandas as pd
import requests
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('C://Users/LENOVO/PycharmProjects/update_db/config.ini')

url = config['paths']['csv_file']
res = requests.get(url)
with open('layer.csv','wb') as file:
    file.write(res.content)
df = pd.read_csv('layer.csv')

def update():
    conn_string = config['database']['connection']
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    select_id = '''SELECT id FROM tbl_layers'''
    cursor.execute(select_id)
    id = cursor.fetchall()
    for j in id:
        id_in_db = (j[0])
        #print(id_in_db)
        for i, row in df.iterrows():
            try:
                if id_in_db == row['id']:
                    sql = '''update tbl_layers set citation=%s, short_description=%s, long_description=%s,
                                         standards=%s, url=%s, display_name=%s, category=%s where id=%s'''

                    cursor.execute(sql, (row['citation'], row['short_description'], row['long_description'],
                                                     row['standards'], row['url'], row['display_name'], row['category'], row['id']))

                    conn.commit()
                else:
                     print("id not found in db")
            except:
                print("message : error occured")
update()