from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import random

# Veritabanı bağlantısı oluşturma
engine = create_engine("mysql+pymysql://remote:BIw883k8@212.31.2.93/monitor",
                           connect_args={"charset": "utf8mb4"}, echo=False)

# Veritabanından sorgulama yapma ve DataFrame oluşturma
df = pd.read_sql("not_indexed", engine)
indexed_df = pd.DataFrame(columns=["index", "Kontrol Durumu", "pubDate", "Datetime"])

def get_google_link(query, pub_date):
    """Google'da verilen sorguyu ara ve sonuçları kontrol et."""

    time = datetime.now()

    url = f"https://www.google.com/search?q={query}&tbm=nws&source=lnt"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }
    #random proxy seçimi
    proxies = {
        'http': ['socks5://dmseo:dmseo@tr-isp1.proxynet.io:7287', 'socks5://dmseo:dmseo@tr-isp1.proxynet.io:7286'],
        'https': ['socks5://dmseo:dmseo@tr-isp1.proxynet.io:7287', 'socks5://dmseo:dmseo@tr-isp1.proxynet.io:7286']
    }
    proxy_type = random.choice(['http', 'https'])
    selected_proxy = random.choice(proxies[proxy_type])

    try:
        response = requests.get(url, headers=headers, proxies={proxy_type: selected_proxy})
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        div = soup.find("div", id="result-stats")

        if not div:
            print(" TEST FAILED ")
            return {"result": False, "data": [query, "Sorun", pub_date, time]}
        else:
            print(" TEST PASSED ")
            return {"result": True, "data": [query, "✓", pub_date, time]}
    except requests.RequestException as e:
        print(f"Hata: {e}")
        return {"result": False, "data": [query, "Hata", pub_date, time]}


def save_to_database(my_df, table_name):
    """DataFrame'i veritabanına kaydet."""
    if not my_df.empty:
        my_df.to_sql(table_name, engine, if_exists="append", index=False)
        
        print("Veritabanına Kaydedildi")

def control_not_indexed():
    # Başarılı işlemleri tutmak için geçici bir DataFrame kullanıyoruz
    successful_indices = []

    for index, row in df.iterrows():
        result_info = get_google_link(row["index"], row['pubDate'])
        if result_info["result"]:
            # Başarılı sonucu indexed_df'e kaydet
            indexed_df.loc[len(indexed_df)] = result_info["data"]
            # Başarılı sonuçların indekslerini sakla
            successful_indices.append(index)
        else:
            # Başarısız sonuçları güncelle
            df.loc[index, ["Kontrol Durumu", "Datetime"]] = [result_info["data"][1], result_info["data"][3]]

    # Başarılı sonuçların indekslerini kullanarak df'den bu satırları çıkar
    df.drop(successful_indices, inplace=True)
    
    save_to_database(indexed_df, "index_checker")
    save_to_database(df[df["Kontrol Durumu"] == "Sorun"], "not_indexed")

if __name__ == "__main__":
    control_not_indexed()
