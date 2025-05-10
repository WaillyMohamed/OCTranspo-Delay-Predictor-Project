import requests #python package to make HTTP requests
from dotenv import load_dotenv # to look at .env file
import csv
import os
import time
import holidays
from google.transit import gtfs_realtime_pb2
from datetime import datetime


# GTFS is a General Transit Feed Specification. the rt is real time so GTSF-RT
#GTFS-RT data is encoded in protocol buffers so not CSV or JSON
# gtfs_realtime_pb2 converts the buffers into readable python objects

# load in keys from env
load_dotenv(dotenv_path='api.env')
API_KEY = os.getenv("OC_API_KEY")
OW_API_KEY = os.getenv("OW_API_KEY") # open weather API key

headers = {"Ocp-Apim-Subscription-Key": API_KEY} # add it to the header

# using the current variable for time
now = datetime.now()
c_holiday = holidays.CA(prov="ON") # check if its a holiday in ontario

# URL for the GTFS-RT trip updates feed 
gtfs_url = 'https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-tp/beta/v1/TripUpdates'

# Method for receiving temperature
def weather_report(city='Ottawa'):
    open_weather_url =  f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OW_API_KEY}&units=metric' # URL for the open weather, current weather api
    response = requests.get(open_weather_url)
    data = response.json()

    #  obtain information
    temperature = data['main']['temp']
    weather_condition = data['weather'][0]['description'] 
    return temperature, weather_condition

# Get temperature
temp, w_condition = weather_report()

# Method for collecting the data
def collect_and_write_data():
    # Fetch the data from the GTFS
    response = requests.get(gtfs_url, headers=headers)  
    # Parse the data
    feed = gtfs_realtime_pb2.FeedMessage() # create empty object of type FeedMessage which can hold trip_update, vehicle, or alert which are all types of feeds we can read from the OCTranspo GTSF-RT feed
    feed.ParseFromString(response.content) # convert the raw data from GTFS-RT feed, now we turn it into a structured python object. Giving the feed variable data it can use.
    
    with open("Delay_Transpo_V2.csv", "a", newline="") as f:
        fieldnames = ['route_id', 'trip_id', 'stop_id', 'arrival_time', 'delay',
                                      'timestamp', 'hour', 'minute', 'day_of_week', 'is_holiday',
                                      'temperature', 'weather_condition']
        
        writer = csv.DictWriter(f, fieldnames=fieldnames) 
        if f.tell() == 0:
            writer.writeheader() # if the header hasnt bee
        for x in feed.entity:
            if x.HasField("trip_update"): # if the feed gives a trip update
                trip = x.trip_update.trip # the actual trip that this message applies to.
                for stopUpdate in x.trip_update.stop_time_update: #stop time update are the updates to Stop Times :)
                    if stopUpdate.HasField("arrival"): # if it has an arrival update
                        is_holiday = int(now.date() in c_holiday) 
                        
                        writer.writerow({
                            'route_id': trip.route_id,
                            'trip_id': trip.trip_id,
                            'stop_id': stopUpdate.stop_id,
                            'arrival_time': stopUpdate.arrival.time,
                            'delay': stopUpdate.arrival.delay,
                            'timestamp': datetime.now().isoformat(),
                            'hour': now.hour,
                            'minute': now.minute,
                            'day_of_week': now.weekday(),
                            'is_holiday': is_holiday,
                            'temperature': temp,
                            'weather_condition': w_condition})

for _ in range (60): # runs 3 times 
    try:
        collect_and_write_data()
        print(f"Collected at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print("Error:", e)
    time.sleep(60) # Wait 60 Seconds
        


