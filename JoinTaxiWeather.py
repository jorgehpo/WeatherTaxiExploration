from pyspark import SparkContext
import datetime
import pytz 

sc = SparkContext(appName="JoinWeatherAndTaxi")

######################################################################
# Weather

#reading weather data
file_weather = sc.textFile("s3://jpo286-ds1004-sp16/Project/datasets/weather-data.txt")

#removing header and splitting " "
file_weather = file_weather.zipWithIndex().filter(lambda (row, index): index > 0).map(lambda (row,index): row.split())

#original format:
# 0      1       2        3   4   5   6   7  8 9 10  11 12 13 14 15 16 17 18 19 20 21   22      23    24    25   26  27   28   29    30     31  32
#USAF  WBAN YR--MODAHRMN DIR SPD GUS CLG SKC L M H  VSB MW MW MW MW AW AW AW AW W TEMP DEWP    SLP   ALT    STP MAX MIN PCP01 PCP06 PCP24 PCPXX SD

#indexing weather by date and selecting interesting columns
def createYearMonthDayHourKey_weather(line):
    utc=pytz.utc
    eastern=pytz.timezone('US/Eastern')
    date = datetime.datetime.strptime(line[2], "%Y%m%d%H%M")
    if date.minute > 30:
        date = date + datetime.timedelta(minutes=30)
    date_gmt = utc.localize(date, is_dst=None)
    date_eastern = date_gmt.astimezone(eastern)
    key = (date_eastern.year, date_eastern.month, date_eastern.day, date_eastern.hour)
    #value = (line[26], line[27])
    value = line
    return(key, value)

file_weather_indexed = file_weather.map(createYearMonthDayHourKey_weather)

# removing and cleaning duplicate hours
# if there is a missing value, search for value in repeated cells
def remove_duplicate_weather(a,b):
    for i in range(len(a)):
        if a[i][0]=='*'
            a[i] = b[i]
    return a;

#weather = file_weather_indexed.reduceByKey(lambda a,b:a, 1)
weather = file_weather_indexed.reduceByKey(remove_duplicate_weather)


######################################################################
# Taxi - time in EST Format


#original format
#   0                1                   2                  3              4                5            6             7            8                  9                    10             11            12     13     14       15           16           17                    18                         
#VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,RateCodeID,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount

#reading taxi data
file_taxi = sc.textFile("s3://jpo286-ds1004-sp16/Project/datasets/yellow_tripdata_2015*")

#removing header and splitting ","
file_taxi = file_taxi.filter(lambda line: line[0:8]!="VendorID").map(lambda row: row.split(","))

# indexing taxi by date
def createYearMonthDayHourKey_taxi(line):
    date = datetime.datetime.strptime(line[1], "%Y-%m-%d %H:%M:%S")
    key = (date.year, date.month, date.day, date.hour)
    value = line
    return(key, value)



taxi = file_taxi.map(createYearMonthDayHourKey_taxi)



######################################################################
# Joining weather and taxi data


# final format - Joined data:
#      0            1            2            3           4             5                      6                    7                8             9               10                11            12                  13                  14               15         16         17      18         19           20                21                  22      23     24         25       26   27  28    29   30  31 32 33   34  35  36  37  38  39  40  41  42  43  44    45       46     47      48   49   50    51     52     53     54   55
# pickup.year, pickup.month, pickup.day, pickup.hour, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, RateCodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, USAF,  WBAN, YR--MODAHRMN, DIR, SPD, GUS, CLG, SKC, L, M, H,  VSB, MW, MW, MW, MW, AW, AW, AW, AW, W, TEMP, DEWP,    SLP,   ALT,    STP, MAX, MIN, PCP01, PCP06, PCP24, PCPXX, SD
 

joined_data = taxi.join(weather)

def toCSVLine(data):
    return ','.join([str(d) for d in data[0]]) + ',' + ','.join([str(d) for d in data[1][0]]) + ',' + ','.join([str(d) for d in data[1][1]]) #joining key and  values

lines_out = joined_data.map(toCSVLine)
lines_out.saveAsTextFile('s3://jpo286-ds1004-sp16/Project/processed_taxi_weather_cleaned.csv')

