from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random
import datetime


serzer = lambda x: dumps(x).encode('utf-8')

# kafka producer object
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=serzer)

stock = [
    ('HPQ', 28.00), ('CSCO', 45.70), ('ZM', 385.20), ('QCOM', 141.10),
    ('ADBE', 476.60), ('VZ', 57.10), ('TXN', 179.40), ('CRM', 240.50),
    ('AVGO', 480.90), ('NVDA', 580.00), ('VMW', 146.80), ('EBAY', 59.40)
]

for s in stock:
    # send the initial stock prices to kafka server
    ticker = s[0]
    price=s[1]
    msg = f'''
        "TICK": {str(ticker)}, "PRICE": {str(price)}, "TS": {str(datetime.datetime.now())}\n'''
    producer.send('StockExchange', value=msg)
    print(msg)


while True:
    # create random prices in the specified random interval for kafka topic
    time.sleep(random.randint(0, 4))
    sl = random.randint(1, len(stock)) - 1
    ticker, price = stock[sl]
    r = random.random() / 10 - 0.5
    price *= 1 + r
    msg = f'''
    "TICK": {str(ticker)}, "PRICE": {str(price)}, "TS": {str(datetime.datetime.now())}\n'''
    producer.send('StockExchange', value=msg)
    print(msg)

