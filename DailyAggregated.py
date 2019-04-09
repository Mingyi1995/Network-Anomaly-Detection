def processTrips(pid, records):
    import csv
    import datetime
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    for row in reader:
        try:
            DOY = str(row[3])+' '+str(row[4])
            date = datetime.datetime.strptime(DOY, '%Y %j').strftime('%d/%m/%Y')
            amount = int(row[6])
            yield((date),amount)
        except ValueError:
            pass
        
from pyspark import SparkContext
if __name__ == "__main__":
    import datetime
    sc = SparkContext()
    rdd = sc.textFile('dc_ride_aggv31.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips).reduceByKey(lambda x,y:x+y).collect()
    print(counts)