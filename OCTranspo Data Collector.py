import requests #python package to make HTTP requests
from dotenv import load_dotenv # to look at .env file
import csv
import os
import time
from google.transit import gtfs_realtime_pb2
from datetime import datetime


# GTFS is a General Transit Feed Specification. the rt is real time so GTSF-RT
#GTFS-RT data is encoded in protocol buffers so not CSV or JSON
# gtfs_realtime_pb2 converts the buffers into readable python objects

# load in key from env
load_dotenv(dotenv_path='api.env')
API_KEY = os.getenv("OC_API_KEY")

headers = {"Ocp-Apim-Subscription-Key": API_KEY} # add it to the header

# URL for the GTFS-RT trip updates feed 
url = 'https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-tp/beta/v1/TripUpdates'
def collect_and_write_data():
    # Fetch the data from the GTFS
    response = requests.get(url, headers=headers)  
    # Parse the data
    feed = gtfs_realtime_pb2.FeedMessage() # create empty object of type FeedMessage which can hold either trip_update, vehicle, or alert which are all types of feeds we can read from the OCTranspo GTSF-RT feed
    feed.ParseFromString(response.content) # convert the raw data from GTFS-RT feed, now we turn it into a structured python object. Giving the feed variable data it can use.

    with open("DelayTranspo.csv", "a", newline="") as f:
        writer = csv.writer(f) 
        for x in feed.entity:
            if x.HasField("trip_update"): # if the feed gives a trip update
                trip = x.trip_update.trip # the actual trip that this message applies to.
                for stopUpdate in x.trip_update.stop_time_update: #stop time update are the updates to Stop Times :)
                    if stopUpdate.HasField("arrival"): # if it has an arrival update
                        writer.writerow([
                            datetime.now().isoformat(), # timestamp for time we collected this data
                            trip.route_id,
                            trip.trip_id,
                            stopUpdate.stop_id,
                            stopUpdate.arrival.time,
                            stopUpdate.arrival.delay])

for _ in range (60): # runs 3 times 
    try:
        collect_and_write_data()
        print(f"Collected at {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print("Error:", e)
    time.sleep(60) # Wait 60 Seconds
        


