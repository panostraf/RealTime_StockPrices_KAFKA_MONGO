import socket
import time
import random
import datetime

PORT = 9999

stock = [
    ('IBM', 123.20), ('AAPL', 125.35), ('FB', 264.30), ('AMZN', 3159.50),
    ('GOOG', 2083.80), ('TWTR', 71.90), ('LNKD', 45.00), ('INTC', 63.20),
    ('AMD', 86.90), ('MSFT', 234.50), ('DELL', 81.70), ('ORKL', 64.70),
    ('HPQ', 28.00), ('CSCO', 45.70), ('ZM', 385.20), ('QCOM', 141.10),
    ('ADBE', 476.60), ('VZ', 57.10), ('TXN', 179.40), ('CRM', 240.50),
    ('AVGO', 480.90), ('NVDA', 580.00), ('VMW', 146.80), ('EBAY', 59.40)
]

ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#ssocket.bind((socket.gethostname(), PORT))
ssocket.bind(('', PORT))
ssocket.listen()
print("Server ready: listening to port {0} for connections.\n".format(PORT))
(c, addr) = ssocket.accept()

for s in stock:
    ticker, price = s
    msg = '{{"TICK": "{0}", "PRICE": "{1:.2f}", "TS": "{2}"}}' \
        .format(ticker, price, datetime.datetime.now())
    print(msg)
    c.send((msg + '\n').encode())

while True:
    time.sleep(random.randint(2, 5))
    sl = random.randint(1, len(stock)) - 1
    ticker, price = stock[sl]
    r = random.random() / 10 - 0.5
    price *= 1 + r
    msg = '{{"TICK": "{0}", "PRICE": "{1:.2f}", "TS": "{2}"}}'\
        .format(ticker, price, datetime.datetime.now())
    print(msg)
    c.send((msg + '\n').encode())

