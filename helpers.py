# This module has been created to cover the needs
# that occur on multiple files,
# such as inv1,inv2 and inv3.

from kafka import KafkaProducer
import json
from datetime import datetime


class PortfolioMonitor:
    # Class to monitor a portfolio through iterations of messages from producer
    def __init__(self,portfolioName,holdings,invName):
        """
        Initialize parameters
        :param portfolioName: <str>
        :param holdings: <dict> of stocks and number of shares per stock {"STOCK":"SHARES"}
        :param invName: <str>
        """
        self.portfolioName = portfolioName
        self.holdings = holdings
        self.invName = invName
        self.NAV = 0 # current NAV
        self.pNAV = 0 # previous NAV

    def updatePortfolio(self,data):
        """
        Main method of the class:
        1) this function gets the price of dictionary data and updates
        the value of holdings dictionary (aka portfolio) by multiplying price * number of shares
        2) then it will update the instance self.NAV with the total evaluation of the portfolio
        (sum of prices)
        3) it will call method send_topic_message to get a json formated message for the producer
        :param data: dictionary with {keys[symbol] : values[price]}
        :return:
        """
        for k,v in data.items():
            # Calculate value for each share
            if k in self.holdings.keys():
                self.holdings[k]['value'] = self.holdings[k]['shares']*v

        # When holdings are updated calls the rest of the functions
        self.updateNAV()
        return self.send_topic_message()


    def updateNAV(self):
        """
        Adds all the values of holdings to estimate portfolio value
        :return: nothing, only updates instance self.NAV
        """
        total = 0
        for key in self.holdings.keys():
            total += self.holdings[key]['value']
        self.NAV = round(total,2)


    def portfolioChange(self):
        """
        A try except is applied to avoid division with 0 in case no previous nav
        was present or if the same price occurs twice
        :return:<float>,<float>
        """
        try:
            change = round(self.NAV - self.pNAV,2)
            change_pct = round(change / self.pNAV,2)
        except ZeroDivisionError:
            change = 0
            change_pct = 0
        return change, change_pct


    def send_topic_message(self):
        """
        given all the instances of the class it will create a message
        :return: <str>
        """
        change, change_pct = self.portfolioChange()
        msg = {
            "investor":self.invName,
            "portfolio": self.portfolioName,
            "nav": self.NAV,
            "change": change,
            "change_pct": change_pct,
            "time": self.cur_time()
        }

        msg = json.dumps(msg)
        self.pNAV = self.NAV
        return msg

    @staticmethod
    def cur_time():
        """
        :return: current time
        """
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M")
        return dt_string


class KafkaPortfolio:
    def __init__(self):
        """
        Initializes a producer object for kafka to be used for different modules with the same settings
        """
        self.host = "localhost:9092"
        self.serzer = lambda x: json.dumps(x).encode('utf-8')
        self.producer = KafkaProducer(bootstrap_servers=[self.host], value_serializer=self.serzer)
        self.topic = 'portfolio'

    def send_data(self,msg):
        """
        Sents the given message to kafka topic
        :param msg: <str>
        :return: nothing
        """
        msg = json.dumps(msg)
        self.producer.send(self.topic,msg)


