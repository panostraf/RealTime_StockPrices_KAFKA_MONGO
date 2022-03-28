#import findspark
#findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,udf,from_unixtime,unix_timestamp
from pyspark.sql.types import DateType
from datetime import datetime

# create a spark session object
spark = SparkSession.builder.master("local[1]")\
                    .appName("app2")\
                    .getOrCreate()

def prtf_stats(INVESTOR,PORTFOLIO,START_DATE,END_DATE):
    """
    Use investor and portfolio attributes to read the correct csv
    uses dates to filter dataset between those periods
    :param INVESTOR: <str>
    :param PORTFOLIO: <str>
    :param START_DATE: <str>
    :param END_DATE: <str>
    :return:
    """
    df = spark.read.csv(f"{INVESTOR}_{PORTFOLIO}.csv",header=False,inferSchema=True).toDF('investor','portfolio','nav','change','changepct','time')
    # Solution 1
    func = udf(lambda x: datetime.strptime(x,"%d/%m/%Y %H:%M"), DateType())
    df.withColumn('date', func(col('time'))).show()
    df = df.withColumn('date', func(col('time')))

    #Solution2
    #df = df.withColumn("date", from_unixtime(unix_timestamp("time",'dd/MM/yyyy HH:mm'),'yyyy-MM-dd').cast(DateType()))

    # Filters the dataframe
    df = df.filter((df.date >= datetime.strptime(START_DATE,"%d/%m/%Y")) & (df.date <= datetime.strptime(END_DATE,"%d/%m/%Y")))

    # get all the given stats
    av = df.agg({"nav":"avg"}).alias("av").collect()[0][0]
    print("Average:", av)

    std = df.agg({"nav":"std"}).alias("std").collect()[0][0]
    print("STD:", std)

    min_ = df.agg({"nav":"min"}).alias("min_").collect()[0][0]
    print("Min:", min_)

    max_ = df.agg({"nav":"max"}).alias("max_").collect()[0][0]
    print("Max", max_)

    try:
        spread = (max_- min_)/av
        print("Spread", spread)
    except TypeError:
        print("Metrics not available, no data for the given period")
        pass
    # and just in case add /0 exception
    except ZeroDivisionError:
        print("Currently there are no data, please try again")
        pass




if __name__=='__main__':
    """
    Iteration through all investors and portolios to provide statistics
    """

    portfolios = {
        "inv1": ['p11', 'p12'],
        "inv2": ['p21', 'p22'],
        'inv3': ['p31', 'p32']
    }
    START_DATE = "1/03/2022"
    END_DATE = "31/03/2022"

    for INVESTOR in portfolios.keys():
        for PORTFOLIO in portfolios[INVESTOR]:
            print("---------------------------------------------")
            print(f"INVESTOR:{INVESTOR} - PORTFOLIO: {PORTFOLIO}")
            prtf_stats(INVESTOR,PORTFOLIO,START_DATE,END_DATE)
            print("\n-------------------------------------------")
