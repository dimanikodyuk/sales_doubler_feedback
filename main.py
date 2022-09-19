import requests
import json
import glob, os
from json import dumps
import config
import pymssql
import time
import urllib
import urllib3
from logs.logs import logger_sales_doubler

conn = pymssql.connect(server=config.host_delta, user=config.user_delta, password=config.password_delta, database=config.database_delta)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def select_lead():
    sel = conn.cursor()
    sel_sql = f"EXEC crm..feedback_sales_doubler;"
    sel.execute(sel_sql)
    res = sel.fetchall()
    return res


def update_lead(p_status, p_lead_id, p_response):
    upd = conn.cursor()
    upd_sql = f"EXEC crm..feedback_sales_doubler_update {p_status}, {p_lead_id}, '{p_response}'"
    upd.execute(upd_sql)


def send_lead(p_trans_id, p_click_id, p_status_id):

    url = f"https://rdr.salesdoubler.com.ua/in/postback/{p_status_id}/{p_click_id}?trans_id={p_trans_id}&token={config.sales_dubler_token}"
    #param = f"trans_id={p_trans_id}&token=ZmlueC5jb20udWFAc2FsZXNkb3VibGVyLmNvbS51YQ"
    param = {"trans_id": p_trans_id,
             "token": "ZmlueC5jb20udWFAc2FsZXNkb3VibGVyLmNvbS51YQ"}

    print(f"URL: {url}")
    response = requests.get(url=url, verify=False)
    print(response.url)

    if response.status_code == 200:
        print(f"LEAD_ID: {p_trans_id} відповідь: {response}")
        update_lead(1, p_trans_id, response.text)
        logger_sales_doubler.info(f"LEAD_ID: {p_trans_id}")
        logger_sales_doubler.info(f"Запит: {url}")
        logger_sales_doubler.info(f"Відповідь: {response}")
    else:
        print(f"LEAD_ID: {p_trans_id} відповідь: {response}")
        logger_sales_doubler.info(f"LEAD_ID: {p_trans_id}")
        logger_sales_doubler.info(f"Запит: {url}")
        logger_sales_doubler.error(f"Відповідь: {response}")


if __name__ == "__main__":
    try:
        res = select_lead()
        for row in res:
            send_lead(row[0], row[1], row[2])

    except ValueError as err:
        logger_sales_doubler.error("Помилка даних main.py: " + str(err))
    except Exception as err:
        logger_sales_doubler.error("Помилка: " + str(err))
