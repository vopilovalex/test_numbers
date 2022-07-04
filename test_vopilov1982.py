# Импорт необходимых модулей
import psycopg2
from psycopg2 import sql
import time
import os
import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import creds
from datetime import datetime

import urllib.request
from xml.dom import minidom

host = "127.0.0.1"
user = "postgres"
password = "adw"
db_name = "Test_Vopilov"

# Scope, который позволяет просматривать, редактировать,\
#удалять или создавать файлы на Google Диске.
SCOPES = ['https://www.googleapis.com/auth/drive']
# Указываем в переменной ACCOUNT_FILE путь к файлу с ключами сервисного аккаунта.
ACCOUNT_FILE = 'vopilov1982-641a45a1d1df.json'

def course():
    # Ежедневные курсы валют ЦБ РФ
    url = "http://www.cbr.ru/scripts/XML_daily.asp"

    # Чтение URL
    webFile = urllib.request.urlopen(url)
    data = webFile.read()
	
    # Имя файла
    UrlSplit = url.split("/")[-1]
    ExtSplit = UrlSplit.split(".")[1]
    FileName = UrlSplit.replace(ExtSplit, "xml")
			
    with open(FileName, "wb") as localFile:
        localFile.write(data)

        webFile.close()

    # Парсинг xml и запись данных в файл
    doc = minidom.parse(FileName)

    # Извлечение даты
    root = doc.getElementsByTagName("ValCurs")[0]
    date = "Текущий курс валют ЦБ РФ на {date}г. \n".format(date=root.getAttribute('Date'))

    # Заголовок CSV
    head = "Идентификатор; Номинал; Название валюты; Сокращение; Курс (руб) \n"

    # Извлечение данных по валютам
    currency = doc.getElementsByTagName("Valute")
    for rate in currency:
        charcode = rate.getElementsByTagName("CharCode")[0]
        value = rate.getElementsByTagName("Value")[0]

        if charcode.firstChild.data == 'USD':
            Text = value.firstChild.data
            Text = Text.replace(',', '.')
            course_USD = (charcode.firstChild.data,round(float(Text), 2))
            print(course_USD)
            return course_USD


# Все обращения к API происходят через эту функцию
def get_service_sacc():
    try:
        creds_json  = ACCOUNT_FILE
        scopes = SCOPES
        creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scopes).authorize(httplib2.Http())
        service = build('sheets', 'v4', http=creds_service)
        sheet = service.spreadsheets()
        sheet_id = "1FqXO-0gcYTV4QzRYsEcVApxl5QaO0_XOJAQUasHsz2E"
        resp = sheet.values().batchGet(spreadsheetId=sheet_id, ranges=["Лист1"]).execute()
        return resp['valueRanges'][0]['values']
    except Exception as e:
        print(str(e))
        pass

# Реализация алгоритма
def main_flow():
    try:
        list_orders = [] 
        orders = get_service_sacc()
        first_entry = True
        for order in orders:

            if first_entry:
                first_entry = False
                continue

            list_orders.append(order[1])

            add_and_change_order_base(order)

        del_order_base(list_orders)    

    except Exception as e:
        print(str(e))
        pass


def add_and_change_order_base(order):
    try:
        connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name)
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute("""SELECT cost_usd, delivery_time FROM orders WHERE order_number = %s"""
                           , [order[1]])
       
            order_from_db = cursor.fetchone()
            if order_from_db:

                if order_from_db[1] != datetime.strptime(order[3], "%d.%m.%Y").date():
                    print('изменяю дату одной строки')
                    cursor.execute("""UPDATE orders SET delivery_time = %s  WHERE order_number = %s""", [order[3],order[1]])

                if float(order_from_db[0]) != float(order[2]):
                    print('изменяю сумму одной строки')
                    course_USD = course()
                    cursor.execute("""UPDATE orders SET cost_usd = %s , cost_rub = %s WHERE order_number = %s""", [order[2],round(float(order[2])*course_USD[1],2),order[1]])

            else:
               print('добавляю одну строку')
               course_USD = course()
               cursor.execute("""INSERT INTO orders (order_number, cost_usd,cost_rub,delivery_time) VALUES (%s,%s,%s,%s)""", (order[1],float(order[2]),round(float(order[2])*course_USD[1],2),order[3]))

    except Exception as ex:
        print(ex)
    finally:
        if connection:
            connection.close()


def del_order_base(list_orders):
    try:
        connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name)
        connection.autocommit = True

        with connection.cursor() as cursor:
            Text = "SELECT order_number FROM orders WHERE order_number NOT IN " + str(list_orders)
            Text = Text.replace('[', '(').replace(']', ')')
            cursor.execute(Text)
            del_orders = cursor.fetchall()
            if del_orders:
                
                for del_order in del_orders:
                    print('Удаляю одну строку')
                    print(del_order[0])
                    Text = "DELETE FROM orders WHERE order_number =" +"'"+del_order[0]+"'"
                    cursor.execute(Text)

    except Exception as ex:
        print(ex)
    finally:
        if connection:
            connection.close()


while(True):
    main_flow()
    time.sleep(5) # Задержка чтобы избежать превышение квоты на запросы
