#This python file is designed to setup a table and populate it with dynamic Weather information from OpenWeatherMaps API.

import sqlalchemy as sqla
from sqlalchemy import create_engine

import urllib
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import requests
import json
import datetime
import time


def dynamicApiCall():
    '''Calls the OpenWeatherMap API to get general Dublin forecast
    due to the close proximity of the stations individual API calls are not being made
    this was initially attempted but frequency of the requests caused constnat scrape crashes;'''

    api_key = '44b78ac274e9d94337e6620489fdcdbe' #Davy's Open Weather Map API Key
    unit = 'metric'
    #station specific calls - for this scaper we will just focus on overall of Dublin
    #api = 'http://api.openweathermap.org/data/2.5/weather?lat=' + str(lat) + '&lon=' + str(long)
    api = 'http://api.openweathermap.org/data/2.5/weather?q=Dublin'
    api_url = api + '&mode=json&units=' + unit + '&APPID=' + api_key
        
    url = urllib.request.Request(api_url)
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
            url = urllib.request.urlopen(api_url)
            output = url.read().decode('utf-8')
            data = json.loads(output)
            url.close()
            
    return data


def  fileBackupWeather(data):  
    '''Dumps loaded json file into local backup file'''
    
    #variables for distinguishing filebackup names
    minutes = str(time.gmtime().tm_min)
    hour = str(time.gmtime().tm_hour)
    day =  str(time.gmtime().tm_mday)
    month =  str(time.gmtime().tm_mon)
    year = str(time.gmtime().tm_year)
                 
    #writes json value to file also includes comma after each value to segment the data from the next installment
    #note this results in an extra comment at end of data but does not appear to cause error
    with open("FileNum-" + str(counter) + "-" + day + "-" + month + "-" + year  + "-Weather-Data-" + hour + "-" + minutes+ ".json","a") as file:
            json.dump(data, file, ensure_ascii=False)
            file.write(",")

    #print("Weather Data Successfully Backed Up!")


def  organisedWeatherData(api_data):
    '''dictionaryData from read Json file - extra work but allows for easier understanding of data'''

    #note: code translates unix time format (seconds)
    
    daylightsavings= 3600
    unix_timestamp  = int(api_data.get('dt') + daylightsavings)
    timeUpdate = str(datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%d-%m-%Y-%H:%M:%S'))
    
    data = dict(
        temp=api_data.get('main').get('temp'),
        temp_max=api_data.get('main').get('temp_max'),
        temp_min=api_data.get('main').get('temp_min'),
        pressure=api_data.get('main').get('pressure'),
        humidity=api_data.get('main').get('humidity'),
        wind=api_data.get('wind').get('speed'), #wind speed
        sky=api_data['weather'][0]['main'], #sky conditions
        dt=timeUpdate #daytime in weird format
        )

    return data


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
    
    sql = "INSERT INTO weatherDynamic VALUES ("  + str(dictionaryData['temp']) + ", " +  str( dictionaryData['temp_max']) +", " + str(dictionaryData['temp_min']) +", " + str(dictionaryData['pressure']) +", " + str(dictionaryData['humidity']) +", "+ str(dictionaryData['wind']) +", '" + str(dictionaryData['sky']) +"', ' " + str( dictionaryData['dt']) + "');"
    
    try:
        res = engine.execute(sql)
    #print(res.fetchall())
    except Exception as e:
        print(e)


def setupTables():
    ''' Sets up station table. Warning: this will also overwrite existing data in the station database'''
    
    sql = """
    CREATE TABLE IF NOT EXISTS weatherDynamic (
    temp REAL,
    temp_max REAL,
    temp_min REAL,
    pressure INTEGER,
    humidity REAL,
    windspeed INTEGER,
    conditions VARCHAR(256),
    time VARCHAR(256)
    )
    """
    try:
        res = engine.execute("DROP TABLE IF EXISTS weatherDynamic")
        res = engine.execute(sql)
        #print(res.fetchall())
    except Exception as e:
        print(e)#, traceback.format_exc())
    

if __name__ == "__main__":
    #connect to database
    engine=DbConnect()

    #sets up or resets station database if it already exists
    #commented out as DB has been setup - do not run again unless want to erase and reset DB
    #setupTables()

    #initialise our counter as we will be gathering data every x number of times
    counter = 0
    
     #timed to execute for total of 7 days (10,080 minutes @ 60 mins per scrape)
    while(counter<168):
    
        #using variables for easier readability
        weatherJson = dynamicApiCall()
        
        #if bikeJson data read is valid
        if weatherJson != "ERROR":

            #note unlike bike data only doing this scrape generally for dublin region for database purposes
            
            #writes jsondata to file as backup in event some weird error occurs.
            fileBackupWeather(weatherJson)
            
            #current stations dictionary data
            weather = organisedWeatherData(weatherJson)

            #writes dictionary data to database
            dynamicDbWrite(weather)
                    
        else:
            print("JSON ERROR - Skipping Scrape")

        #increment our counter in while loop - sentinel value
        #in the event the code is forgotten to be manually switched off it will terimate after 168 loops
        counter += 1
        print("Finished loop: " + str(counter) + "/168")

        #60 minute sleep
        time.sleep(3600)
        
    print("Finished Scraping")


