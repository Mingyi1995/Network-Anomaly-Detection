def processTrips(pid, records):
    import csv
    import datetime
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    for row in reader:
        try:
            DOY = str(row[1])
            date = datetime.datetime.strptime('2018-01-01 00:24:39', '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')
            amount = int(row[6])
            yield((date),amount)
        except ValueError:
            pass
        
from pyspark import SparkContext
if __name__ == "__main__":
    import datetime
    sc = SparkContext()
    rdd = sc.textFile('NewYork_Taxi.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips).reduceByKey(lambda x,y:x+y).saveAsTextFile('DailyAggregatedNewYork')
    print(done)