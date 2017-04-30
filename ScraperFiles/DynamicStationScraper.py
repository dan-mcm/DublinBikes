#This python file is designed to setup a table and populate it with static Dublin Bike station information.

import sqlalchemy as sqla
from sqlalchemy import create_engine

import urllib
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import requests
import json
import time


def dynamicApiCall():
    '''Calls the Dublin Bikes API to get all live station data in one go
    added try except methods to handle errors that may occur when going to scrape the data
    returns info as json or string stating ERROR which is checked in main'''
    
    api_key = "c64916b14c557faa49fdf72b8902e4d9ff9afe35"
    url = urllib.request.Request("https://api.jcdecaux.com/vls/v1/stations?contract=Dublin&apiKey=" + api_key)
    try:
            urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
            #if http error were going to return an ERROR string
            data = "ERROR"
            print(e)
    except urllib.error.URLError as e:
            #if server error were going to return an ERROR string
            data = "ERROR"
            print(e)
    except socket.error as e:
            #extra socket error check as this crashed weather api
            data = "ERROR"
            print(e)
    except Exception as e:
            #mimicing comfirmed working error exception catch as extra precaution
            data = "ERROR"
            print(e)

    else:
            url = urllib.request.urlopen(url)
            output = url.read().decode('utf-8')
            data = json.loads(output)
            url.close()
    return data


def  fileBackupBikes(data):  
    '''Dumps loaded json file into local backup file'''
    
    #variables for distinguishing filebackup names
    minutes = str(time.gmtime().tm_min)
    hour = str(time.gmtime().tm_hour)
    day =  str(time.gmtime().tm_mday)
    month =  str(time.gmtime().tm_mon)
    year = str(time.gmtime().tm_year)

    with open("FileNum-" + str(counter) + "-" + day + "-" + month + "-" + year  + "-Dynamic-Station-Data" + hour + "-" + minutes +".json","w") as file:
        json.dump(data, file, ensure_ascii=False)

    #print("Bike Data Successfully Backed Up!")


def  organisedBikeData(data,i):
    '''dictionaryData from read Json file - extra work but allows for easier understanding of data'''
    #note: code translates time since epoch (milliseconds)
    #adding an hour in seconds to epoch time
        
    daylightsavings= 3600000
    y = int(data[i].get('last_update') + daylightsavings)
    x = time.gmtime(y/1000)
    timeUpdate = str(time.strftime('%d-%m-%Y-%H:%M:%S', x))
        
    dictionaryData = dict(
            stationNumber=  data[i].get('number'),
            stationName= data[i].get('name'),
            stationLat= data[i].get('position').get('lat'),
            stationLong= data[i].get('position').get('lng'),
            stationStatus=  data[i].get('status'),
            stationBikeStands=  data[i].get('bike_stands'),
            stationAvailableStands=  data[i].get('available_bike_stands'),
            stationAvailableBikes=  data[i].get('available_bikes'),
            lastUpdate= timeUpdate
    )
          
    return dictionaryData


def DbConnect():
    '''Establishes connection to database '''
    
    #connection inside of dbWrite for self-continament of connection establishment - easier to implement
    URI = "dublinbikes.cns5jzditmzn.us-west-2.rds.amazonaws.com" #link to AWS hosted RDS
    PORT = "3306" #default port on RDS
    DB = "dublinbikes" #simple DB name - not built for security
    USER = "dublinbikes" # simple user name - not built for security
    PASSWORD="dublinbikes" #simple password - not built for security

    engine = create_engine("mysql+mysqldb://{}:{}@{}:{}/{}".format(USER, PASSWORD, URI, PORT, DB), echo=True)
    return engine


def dynamicDbWrite(dictionaryData):
    '''inserts key static station data values into DB
    note special case for stationName as varchar
    does not handle character symbols'''
    
    sql = "INSERT INTO stationDynamic VALUES (" + str(dictionaryData['stationNumber']) +", '" + str(dictionaryData['stationStatus']) + "'," + str(dictionaryData['stationAvailableBikes']) + "," + str(dictionaryData['stationAvailableStands']) + ", '" + str(dictionaryData['lastUpdate']) + "');"
    
    try:
        res = engine.execute(sql)
    #print(res.fetchall())
    except Exception as e:
        print(e)


def setupTables():
    ''' Sets up station table. Warning: this will also overwrite existing data in the station database'''
    
    sql = """
    CREATE TABLE IF NOT EXISTS stationDynamic (
    number INTEGER,
    status VARCHAR(256),
    available_bikes INTEGER,
    available_bike_stands INTEGER,
    last_update VARCHAR(256)
    )
    """
    try:
        res = engine.execute("DROP TABLE IF EXISTS stationDynamic")
        res = engine.execute(sql)
        #print(res.fetchall())
    except Exception as e:
        print(e)#, traceback.format_exc())
    

if __name__ == "__main__":
    #connect to database
    engine=DbConnect()

    #sets up or resets station database if it already exists
    #commented out as DB has been setup - do not run again unless you want to wipe DB
    #setupTables()

    #initialise our counter as we will be gathering data every x number of times
    counter = 0
    
     #timed to execute for total of 7 days (10,080 minutes @ 5mins per scrape)
    while(counter<2016):
    
        #using variables for easier readability
        bikeJson = dynamicApiCall()
        
        #if bikeJson data read is valid
        if bikeJson != "ERROR":
            
            #writes jsondata to file as backup in event some weird error occurs.
            fileBackupBikes(bikeJson)

            #cycling through all the stations
            for i in range(0,101):

                #current stations dictionary data
                station = organisedBikeData(bikeJson,i)

                #writes dictionary data to database
                dynamicDbWrite(station)

                #mimic progress counter - allows for easy tracking of progress for live debugging
                print(i, "%")
                    
        else:
            print("JSON ERROR - Skipping Scrape")

        #increment our counter in while loop - sentinel value
        #note this is approximate as the scraping takes a couple minutes each turn
        #most likely a manual close of the script will be needed however this is a built in cut off
        #in the event the code is forgotten to be manually switched off it will terimate after 2016 loops
        counter += 1
        print("Finished loop: " + str(counter) + "/2016")

        #5 minute sleep
        time.sleep(300)
        
    print("Finished Scraping")


