#This python file is designed to setup a table and populate it with static Dublin Bus station information.

import sqlalchemy as sqla
from sqlalchemy import create_engine

import urllib
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import requests
import json


def staticApiCall():
    '''Calls the Dublin Bikes API to get all station data in one go
    added try except methods to handle errors that may occur when going to scrape the data
    returns info as json'''
    
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
    
    with open("Static-Station-Data.json","w") as file:
        json.dump(data, file, ensure_ascii=False)

    #print("Bike Data Successfully Backed Up!")


def  organisedBikeData(data,i):
    '''dictionaryData from read Json file - extra work but allows for easier understanding of data'''
    dictionaryData = dict(
        stationNumber=  data[i].get('number'),
        stationName= data[i].get('name'),
        stationLat= data[i].get('position').get('lat'),
        stationLong= data[i].get('position').get('lng'),
        stationStatus=  data[i].get('status'),
        stationBikeStands=  data[i].get('bike_stands')
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


def staticDbWrite(dictionaryData):
    '''inserts key static station data values into DB
    note special case for stationName as varchar
    does not handle character symbols'''
    
    sql = "INSERT INTO stationStatic VALUES (" + str(dictionaryData['stationNumber']) +", '" + str(''.join(e for e in dictionaryData['stationName'] if e.isalnum())) + "'," + str(dictionaryData['stationLat']) + "," + str(dictionaryData['stationLong']) + "," + str(dictionaryData['stationBikeStands']) + ");"
    
    try:
        res = engine.execute(sql)
    #print(res.fetchall())
    except Exception as e:
        print(e)


def setupTables():
    ''' Sets up station table. Warning: this will also overwrite existing data in the station database'''
    
    sql = """
    CREATE TABLE IF NOT EXISTS stationStatic (
    number INTEGER,
    name VARCHAR(256),
    position_lat REAL,
    position_lng REAL,
    bike_stands INTEGER
    )

    """
    try:
        res = engine.execute("DROP TABLE IF EXISTS stationStatic")
        res = engine.execute(sql)
        #print(res.fetchall())
    except Exception as e:
        print(e)
    

if __name__ == "__main__":
    #connect to database
    engine=DbConnect()

    #sets up or resets station database if it already exists
    #commented out as DB has been setup
    #setupTables()

    #using variables for easier readability
    bikeJson = staticApiCall()

    #if bikeJson data read is valid
    if bikeJson != "ERROR":
        
        #writes jsondata to file as backup in event some weird error occurs.
        fileBackupBikes(bikeJson)

        #cycling through all the stations
        for i in range(0,101):

            #current stations dictionary data
            station = organisedBikeData(bikeJson,i)

            #writes dictionary data to database
            staticDbWrite(station)

            #mimic progress counter - allows for easy tracking of progress for live debugging
            print(i, "%")
                
    else:
        print("JSON ERROR")
        
    #print statement to indicate program has finished
    print("Finished!")


