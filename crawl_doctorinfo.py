# -*- coding: utf-8 -*-

import requests, html5lib, time, random
import mysql.connector
from bs4 import BeautifulSoup
from random import randint

import sys
reload(sys)
sys.setdefaultencoding('utf8')



# database info
username = 'root'
password = '123456'
host = 'localhost'
dbase = 'doctorinfo'





def get_doctor_info():

    dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase, charset='utf8')
    cursor = dbconn.cursor()

    # change the cId to choose start which doctor
    query = ("select NPI, LastName, FirstName, MiddleName, Primaryspecialty, City from physicians_sample where cId > 35 order by cId limit 5"
            )
    cursor.execute(query)


    doctor_infolist = []

    for (NPI, last_name, first_name, middle_name, primary_specialty, city) in cursor:

        # most results: fistname is before lastname
        if middle_name == None:
            doctor_info = "{} {} {} {}#{}".format( first_name, last_name, primary_specialty, city, NPI)
        else:
            doctor_info =  "{} {} {} {} {}#{}".format(first_name, middle_name, last_name, primary_specialty, city, NPI)
        print(doctor_info)
        doctor_infolist.append(doctor_info)

    cursor.close()

    return doctor_infolist


def get_proxy_address():
    dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase, charset='utf8')
    cursor = dbconn.cursor()

    query = ("select proxyAddress from proxy_addresses where addressValidity > 0 order by rand() limit 1"
            )
    cursor.execute(query)


    for (cursor_data) in cursor:

        proxy_address = cursor_data[0]
        print('Proxy address:' + proxy_address)

    cursor.close()

    return proxy_address

def update_proxdy_address(proxy_address, address_validity):

    dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase, charset='utf8')
    cursor = dbconn.cursor()

    if address_validity == 0:

        args = (proxy_address,)
        cursor.callproc('update_proxy', args)

    cursor.close()


# get user-agent set 'uafile' as your path of 'user_agents.txt'
def get_header():

    uafile = "C:/Users/Yanbo/Documents/Code/Python/crawlDoctorInfo/user_agents.txt"

    uas = []
    with open(uafile, 'rb') as uaf:
        for ua in uaf.readlines():

            uas.append(ua.strip()[1:-1 - 1])
    header = random.choice(uas)

    return header


def crawl(doctor_info, page_number):

    dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase, charset='utf8')
    cursor = dbconn.cursor()

    print('-------------Begin Crawl-------------')

    keyword = doctor_info.split('#')[0]
    NPI = doctor_info.split('#')[1]


    # first page start=0, second page start=10
    url = "https://www.google.com/search?q=%s&start=%s" % (keyword, (page_number-1)*10)
    print("SearchURL: " + url)

    proxy_address = get_proxy_address()

    # sleep time
    second = random.randrange(5, 40)
    time.sleep(second)

    user_agent = get_header()

    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip,deflate,sdch',
        'Accept-Language': 'en-US',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'User-Agent': user_agent
    }

    proxies = {
        'http': 'http://%s' % (proxy_address)
    }

    try :

        wb_data = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        content = wb_data.content.decode('utf8', 'ignore')
        soup = BeautifulSoup(content, 'html5lib')

        # print(soup.prettify())
    except Exception as error:
        print(error)
        update_proxdy_address(proxy_address, 0)
        crawl(doctor_info, page_number)


    j = 1

    try:


        for result_data in soup.select(".g"):

            if j == 11:

                break


            result_title = result_data.select('h3 > a')[0].text.strip()
            result_url = result_data.select('h3 > a')[0].get('href')

            if result_url[0] == '/':
                result_url = result_url.split('?q=')[1].split('&sa')[0]
            if result_url[11:17] == "google":
                result_url = result_url.split('?url=')[1].split('&rct')[0]


            if j == 10:

                result_rank = str(page_number) + str(j)

                args = (NPI,)
                cursor.callproc("update_crawlFlag", args)

            else:
                result_rank = str(page_number) + '0' + str(j)

            j+=1

            print(' Keyword: %s \n Rank: %s \n Title: %s \n Url: %s' % (keyword, result_rank, result_title, result_url))



            try:

                args = (NPI, keyword, result_rank, result_title, result_url)
                add_google_results = ("INSERT IGNORE INTO google_results "
               "(doctorNPI, searchKeyword, searchResultRank, searchResultTitle, searchResultUrl) "
               "VALUES (%s, %s, %s, %s, %s)")
                cursor.execute(add_google_results, args)

                dbconn.commit()

            except mysql.connector.Error as error:
                print(error)

    except Exception as error:

            print(error)
            print('-------------Crawl again-------------')

            cursor.close()
            dbconn.close()

            update_proxdy_address(proxy_address, 0)
            crawl(doctor_info, page_number)


    cursor.close()
    dbconn.close()


def main():
    doctor_data = get_doctor_info()
    for i in range(len(doctor_data)):
        doctor_info = doctor_data[i]
        print(i+1, doctor_info)
        for page_number in (1,2):
            crawl(doctor_info, page_number)


if __name__ == '__main__':
    main()


'''
healthgrade website:

https://www.healthgrades.com
https://www.ratemds.com/
http://www.topnpi.com/

'''

