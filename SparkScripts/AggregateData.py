from pyspark import SparkContext
import datetime as dt
import json
import math
import os

# final format - Joined data:
#      0            1            2            3           4             5                      6                    7                8             9               10                11            12                  13                  14               15         16         17      18         19           20                21                  22      23     24         25       26   27  28    29   30  31 32 33   34  35  36  37  38  39  40  41  42  43  44    45       46     47      48   49   50    51     52     53     54   55
# pickup.year, pickup.month, pickup.day, pickup.hour, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, RateCodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, USAF,  WBAN, YR--MODAHRMN, DIR, SPD, GUS, CLG, SKC, L, M, H,  VSB, MW, MW, MW, MW, AW, AW, AW, AW, W, TEMP, DEWP,    SLP,   ALT,    STP, MAX, MIN, PCP01, PCP06, PCP24, PCPXX, SD



####people said to divide manhattan in 10meters
####

#getting spark context
sc = SparkContext(appName="AggregateDataTaxiWeather")

#reading joined data
dataframe = sc.textFile('s3://jpo286-ds1004-sp16/Project/processed_taxi_weather.csv/*')

#splitting file by commas and removing header
dataframe = dataframe.zipWithIndex().filter(lambda (row, index): index > 0).map(lambda (row,index): row.split(','))

#removing instances which have outliers of latitude and logitude and no temperature
def outlier_lat_long_detection(row):
    #new york state boundaries:
    #Coord:   41.507548, -74.645228          
    min_latitude, max_latitude, min_longitude, max_longitude = (40.5, 40.91, -74.26, -73.69)
    temperature = row[44]
    pickup_longitude =  float(row[9])
    pickup_latitude =   float(row[10])
    dropoff_longitude = float(row[13])
    dropoff_latitude =  float(row[14])
    return (pickup_longitude >= min_longitude and pickup_longitude <= max_longitude and pickup_latitude >= min_latitude and pickup_latitude <= max_latitude       and        dropoff_longitude >= min_longitude and dropoff_longitude <= max_longitude and dropoff_latitude >= min_latitude and dropoff_latitude <= max_latitude and temperature[0] != '*')


dataframe = dataframe.filter(outlier_lat_long_detection)

#finding min and max values of lattitude and longitude
pickup_longitude = dataframe.map(lambda row:float(row[9]))
pickup_latitude = dataframe.map(lambda row:float(row[10]))
dropoff_longitude = dataframe.map(lambda row:float(row[13]))
dropoff_latitude = dataframe.map(lambda row:float(row[14]))

longitude = pickup_longitude.union(dropoff_longitude)
latitude = pickup_latitude.union(dropoff_latitude)

min_longitude = longitude.reduce(lambda a,b:min(a,b))
min_latitude = latitude.reduce(lambda a,b:min(a,b))
max_longitude = longitude.reduce(lambda a,b:max(a,b))
max_latitude = latitude.reduce(lambda a,b:max(a,b))

#divinding space into 10 meter squares

def spherical_distance(lat1,long1,lat2,long2):
    #Haversine formula
    #http://www.movable-type.co.uk/scripts/latlong.html
    R = 6371000.
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2-lat1)
    delta_lambda = math.radians(long2-long1)
    a = math.sin(delta_phi/2.) * math.sin(delta_phi/2.) + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2.) * math.sin(delta_lambda/2.)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return (R * c)

#each "bin" is about 500x500 meters
nbins_latitude = int(spherical_distance(min_latitude,0,max_latitude,0)/500.)
nbins_longitude = int(spherical_distance(0,min_longitude,0,max_longitude)/500.)

size_bin_latitude = (max_latitude - min_latitude)/nbins_latitude
size_bin_longitude = (max_longitude - min_longitude)/nbins_longitude



#creating key-value pairs for aggregation
def key_value_heatmap_aggregation(row, typeOp = 'pickup'): #typeOp:pickup or dropoff
    month = int(row[1])
    weekday = dt.date(int(row[0]),int(row[1]),int(row[2])).weekday()
    hour = int(int(row[3]) / 6) #0-4, 4-8, 8-12, 12-16, 16-20, 20-24
    weather_list = row[35:43]
    for i in range(8):
        if weather_list[i][0] == "*":
            weather_list[i] = 0
        else:
            weather_list[i] = int(weather_list[i])
    weather_id = max(weather_list)
    weather = 'ShoweryPrecipitation'
    if weather_id < 50:
        weather = 'NoPrecipitation'
    elif weather_id < 60:
        weather = 'Drizzle'
    elif weather_id < 70:
        weather = 'Rain'
    elif weather_id < 80:
        weather = 'SolidPrecipitation'
    temperature = max(0,int(row[44])/20+1) #-20 - 0, 0-20, 20-40, 40-60, 60-80, 80-100 F
    if type == 'pickup':
        longitude =  float(row[9])
        latitude =   float(row[10])
    else:
        longitude = float(row[13])
        latitude =  float(row[14])
    bin_latitude = int((latitude - min_latitude)/size_bin_latitude)
    bin_longitude = int((longitude - min_longitude)/size_bin_longitude)
    #        0       1       2      3          4            5             6            7
    key = (month, weekday, hour, weather, temperature, bin_latitude, bin_longitude, typeOp)
    return (key, 1)


key_value_map_pickup = lambda row:key_value_heatmap_aggregation(row, typeOp = 'pickup')
key_value_map_dropoff = lambda row:key_value_heatmap_aggregation(row, typeOp = 'dropoff')

mapped_pickups = dataframe.map(key_value_map_pickup)
mapped_dropoffs = dataframe.map(key_value_map_dropoff)

mapped_total = mapped_pickups.union(mapped_dropoffs)

occurrence_by_index = mapped_total.reduceByKey(lambda a,b:a+b)



#creating 2-D histograms for map.
#Mapping data to new key-value format:

def key_value_final_histogram(row):
    key = row[0]
    count = row[1]
    newKey = tuple([key[i] for i in [0, 1, 2, 3, 4, 7]])
    newValue = [key[5], key[6], count] #bin latitude, bin longitude and count
    return (newKey, newValue)


indexed_histogram = occurrence_by_index.map(key_value_final_histogram)

grouped_histogram = indexed_histogram.groupByKey()

data_retrieved = grouped_histogram.collect()

output_folder = "."

#Saving everything

output = {}

output['parameters'] = {
    'min_longitude':min_longitude,
    'max_longitude':max_longitude,
    'min_latitude':min_latitude,
    'max_latitude':max_latitude,
    'nbins_latitude':nbins_latitude,
    'nbins_longitude':nbins_longitude,
    'size_bin_latitude':size_bin_latitude,
    'size_bin_longitude':size_bin_longitude
}

output['queries'] = []

#2-D histogram data files:

for data in data_retrieved:
    (month, weekday, hour, weather, temperature,typeOp) = data[0] #key
    current_query = {}
    current_query['month'] = month
    current_query['weekday'] = weekday
    current_query['hour'] = hour
    current_query['weather'] = weather
    current_query['temperature'] = temperature
    current_query['typeOp'] = typeOp
    current_query['heatmap'] = list(data[1])
    output['queries'].append(current_query)

fname = "taxi_weather_queries.json"
f = open(os.path.join(output_folder, fname),'w')
json.dump(output, f)
f.close()
