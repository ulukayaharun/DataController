import os
import requests
from bs4 import BeautifulSoup
from ftplib import FTP
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import random

# Sabit değerlerin tanımlanması 
FTP_SERVER = "ftpupload.net"
FTP_USER = "if0_35194504"
FTP_PASSWORD = "45SFkL7u3P3EtaR"
LOCAL_FOLDER = "newssitemaps"
EXCLUDE_FILES = ["sozcu.xml", "fotomac.xml", "haberler.xml", "haberturk.xml", "mynet.xml", "takvim.xml", "sabah.xml"]

df = pd.DataFrame(columns=["Kontrol Durumu", "pubDate", "Datetime"])
notdf = pd.DataFrame(columns=["Kontrol Durumu", "pubDate", "Datetime"])
engine = create_engine("mysql+pymysql://remote:BIw883k8@212.31.2.93/monitor",
                           connect_args={"charset": "utf8mb4"}, echo=False)

def ftp_reach_xml():
    """FTP'den XML dosyalarını indir."""
    try:
        #ftp bağlantısı
        ftp = FTP(FTP_SERVER)
        ftp.login(user=FTP_USER, passwd=FTP_PASSWORD)
        ftp.cwd("htdocs")
        os.makedirs(LOCAL_FOLDER, exist_ok=True)
        files = ftp.nlst()

        for file in files:
            if file.endswith(".xml") and file not in EXCLUDE_FILES:  
                path = os.path.join(LOCAL_FOLDER, file)
                with open(path, "wb") as local_file:
                    ftp.retrbinary("RETR " + file, local_file.write)

        ftp.quit()
        print("XML dosyaları indirildi.")
    except Exception as e:
        print("Bir hata oluştu:", e)

def find_write_pubDate():
    """XML dosyalarından pubDate bilgilerini çek ve kontrol et."""

    checked_links = set()
    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name.endswith(".xml"):
            path = os.path.join(LOCAL_FOLDER, file_name)
            with open(path, "r", encoding="utf-8") as xml_file:
                soup = BeautifulSoup(xml_file, "xml")
                for item in soup.find_all('item'):
                    link = item.find('link').text
                    if link not in checked_links:
                        pub_date_str = item.find('pubDate').text
                        date_obj = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z')
                        pub_date=date_obj.strftime("%Y-%m-%d %H:%M:%S")
                        get_google_link(link, pub_date)
                        checked_links.add(link)
    print("XML dosyaları Hazılandı ")

def get_google_link(query: str, pubDate: datetime):
    """Google'da verilen sorguyu ara ve sonuçları kontrol et."""
    time = datetime.now() #Kontrol edilme anı

    #random proxy seçimi
    proxies = {
        'http': ['socks5://dmseo:dmseo@tr-isp1.proxynet.io:7287', 'socks5://dmseo:dmseo@tr-isp1.proxynet.io:7286'],
        'https': ['socks5://dmseo:dmseo@tr-isp1.proxynet.io:7287', 'socks5://dmseo:dmseo@tr-isp1.proxynet.io:7286']
    }
    proxy_type = random.choice(['http', 'https'])
    selected_proxy = random.choice(proxies[proxy_type])

    url = f"https://www.google.com/search?q={query}&tbm=nws&source"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }
    try:
        html = requests.get(url, headers=headers,proxies={proxy_type: selected_proxy}).content
        soup = BeautifulSoup(html, 'html.parser')
        div = soup.find("div", id="result-stats")

        #result-stats üzerinden Dataframe güncellenmesi
        if not div:
            notdf.loc[query] = ["Sorun", pubDate, time]
            print(" TEST FAILED ")

        else:
            df.loc[query] = ["✓", pubDate, time]
            print(" TEST PASSED ")
    except Exception as e:
        print("Bir hata oluştu " + e)


def save_to_database(my_df, table_name:str):
    """DataFrame'i veritabanına kaydet."""

    if not my_df.empty:
        my_df.to_sql(table_name, engine, if_exists="append")

        my_df=pd.DataFrame(columns=["Kontrol Durumu", "pubDate", "Datetime"]) # veritabanına kaydettikten sonra dataframei sıfırlar
        
        print("Veritabanına Kaydedildi")

if __name__ == "__main__":
    ftp_reach_xml()
    find_write_pubDate()
    save_to_database(df,"index_checker")
    save_to_database(notdf,"not_indexed")

