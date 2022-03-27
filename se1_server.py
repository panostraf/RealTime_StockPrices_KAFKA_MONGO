
from json import dumps
from kafka import KafkaProducer
import time
import random
import datetime


serzer = lambda x: dumps(x).encode('utf-8')

# producer object
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serzer)

stock = [
    ('IBM', 123.20), ('AAPL', 125.35), ('FB', 264.30), ('AMZN', 3159.50),
    ('GOOG', 2083.80), ('TWTR', 71.90), ('LNKD', 45.00), ('INTC', 63.20),
    ('AMD', 86.90), ('MSFT', 234.50), ('DELL', 81.70), ('ORKL', 64.70)
]


for s in stock:
    # send the initial values to kafka topic
    ticker = s[0]
    price=s[1]
    msg = f'''
        "TICK": {str(ticker)}, "PRICE": {str(price)}, "TS": {str(datetime.datetime.now())}\n'''
    producer.send('StockExchange', value=msg)
    print(msg)



while True:
    # send random prices to kafka topic
    time.sleep(random.randint(0, 4))
    sl = random.randint(1, len(stock)) - 1
    ticker, price = stock[sl]
    r = random.random() / 10 - 0.5
    price *= 1 + r
    msg = f'''
    "TICK": {str(ticker)}, "PRICE": {str(price)}, "TS": {str(datetime.datetime.now())}\n'''
    producer.send('StockExchange', value=msg)
    print(msg)

