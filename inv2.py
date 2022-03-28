from json import loads
import json
from kafka import   KafkaConsumer, KafkaProducer
import time
from collections import defaultdict
import ast
import helpers
import threading
dezer = lambda x: loads(x.decode('utf-8'))

consumer = KafkaConsumer('StockExchange',
                        bootstrap_servers=['localhost:9092'],
                        auto_offset_reset='latest',
                        enable_auto_commit=False,
                        group_id='inv2'
                        #value_deserializer=lambda m: loads(m.decode('utf-8'))
                         )

# Set portfolio shares and stocks
p21 = {
    "IBM":{"shares":1000,"value": 0},
    "AAPL":{"shares":1200,"value": 0},
    "FB": {"shares":900,"value": 0},
    "AMZN":{"shares":2300,"value": 0},
    "GOOG":{"shares":1400,"value": 0},
    "TWTR":{"shares":400,"value": 0}
}

p22 = {
    "LNKD":{"shares":900,"value": 0},
    "INTC":{"shares":600,"value": 0},
    "AMD":{"shares":1100,"value": 0},
    "MSFT":{"shares":1200,"value": 0},
    "DELL":{"shares":700,"value": 0},
    "ORCL":{"shares":1200,"value": 0},
}


prtf = defaultdict(lambda :0)

# initialize class to produce messages to kafka topic
port_obj = helpers.KafkaPortfolio()

# Initialize classes for portolio monitoring
p21 = helpers.PortfolioMonitor("p21",p21,'inv2')
p22 = helpers.PortfolioMonitor("p22",p22,'inv2')

while True:
    messages = consumer.poll() # kafka obj
    for msg in messages.values():
        for row in msg: # read data
            data = eval(row.value).split(",") # Split string
            ticker = data[0].split(":")
            ticker = ticker[1].strip()

            price = data[1].split(":")
            price = float(price[1].strip())

            prtf[ticker] = price # Add price to ticker

        consumer.pause() # Pause connection
        print("p21 updated")
        p1MSG = p21.updatePortfolio(prtf)
        port_obj.send_data(p1MSG)
        print("p22 updated")
        p2MSG = p22.updatePortfolio(prtf)
        port_obj.send_data(p2MSG)

        time.sleep(20)  # sleep 10 sec
        consumer.resume()  # Resume to connection from last point seen
