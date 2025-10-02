from confluent_kafka import Producer
import requests, json, time

# kafka config
conf = {
    'bootstrap.servers': "XXXX",   
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': "XXXX",
    'sasl.password': "XXXX"
}

producer = Producer(conf)

# API endpoint 
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum,solana",
    "x_cg_demo_api_key": "XXXXXX"
}

while True:
    try:
        resp = requests.get(url, params=params, timeout=10)  # 10 sec timeout
        if resp.status_code == 200:
            data = resp.json()
            for coin in data:
                msg = {
                    "id": coin["id"],
                    "symbol": coin["symbol"],
                    "image": coin["image"],  
                    "price": coin["current_price"],
                    "market_cap": coin["market_cap"],
                    "market_cap_rank": coin["market_cap_rank"],
                    "volume": coin["total_volume"],
                    "high_24h": coin["high_24h"],
                    "low_24h": coin["low_24h"],
                    "price_change_24h": coin["price_change_24h"],
                    "price_change_percentage_24h": coin["price_change_percentage_24h"],
                    "circulating_supply": coin["circulating_supply"],
                    "total_supply": coin["total_supply"],
                    "max_supply": coin["max_supply"],
                    "ath": coin["ath"],
                    "ath_change_percentage": coin["ath_change_percentage"],
                    "ath_date": coin["ath_date"],
                    "atl": coin["atl"],
                    "atl_change_percentage": coin["atl_change_percentage"],
                    "atl_date": coin["atl_date"],
                    "last_updated": coin["last_updated"]
                }
                producer.produce("real_time", value=json.dumps(msg))  
                print("Sent:", msg)
        else:
            print("Error:", resp.status_code, resp.text)

        producer.flush()  # ensure delivery
        time.sleep(2)     # ~30 calls/min
    except Exception as e:
        print(" Error fetching API:", e)
        time.sleep(5)