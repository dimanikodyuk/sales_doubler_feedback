import requests
import json
import glob, os
from json import dumps
import config
import pymssql
import time
import urllib
import urllib3

conn = pymssql.connect(server=config.host_delta, user=config.user_delta, password=config.password_delta, database=config.database_delta)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def select_lead():
    sel = conn.cursor()
    sel_sql = f"EXEC crm..feedback_sales_doubler;"
    sel.execute(sel_sql)
    res = sel.fetchall()

    return res

def send_lead(p_trans_id, p_click_id, p_status_id):

    url = f"https://rdr.salesdoubler.com.ua/in/postback/{p_status_id}/{p_click_id}?trans_id={p_trans_id}&token=ZmlueC5jb20udWFAc2FsZXNkb3VibGVyLmNvbS51YQ"
    #param = f"trans_id={p_trans_id}&token=ZmlueC5jb20udWFAc2FsZXNkb3VibGVyLmNvbS51YQ"
    param = {"trans_id": p_trans_id,
             "token": "ZmlueC5jb20udWFAc2FsZXNkb3VibGVyLmNvbS51YQ"}
    #response = requests.get(url=url, params=param, verify=False)

    print(f"URL: {url}")
    response = requests.get(url=url, verify=False)
    print(response.url)

    if response.status_code == 200:
        print("SUCCESFULY")
        print(response.text)
    else:
        print("ERROR")
        print(response)

if __name__ == "__main__":
    res = select_lead()
    for row in res:
        send_lead(row[0], row[1], row[2])
