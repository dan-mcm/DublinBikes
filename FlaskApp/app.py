from flask import Flask, g, render_template, jsonify
from flask_googlemaps import GoogleMaps, Map, icons
from sqlalchemy import create_engine
import config
import json
import requests

app = Flask(__name__)

#set your googlemaps api key
app.config['GOOGLEMAPS_KEY']="AIzaSyB4ipnj_JcaXKHFruw172nwitgJWdcV9Fk" #Daniel McMahon's Key
#initalize the extension
GoogleMaps(app)

def connect_to_database():
    db_str = "mysql+mysqldb://{}:{}@{}:{}/{}"
    engine = create_engine(db_str.format(config.USER,
                                        config.PASSWORD,
                                        config.URI,
                                        config.PORT,
                                        config.DB),
                           echo=True)

    return engine

def get_db():
    '''function sets up database connection'''
    engine = getattr(g, 'engine', None)
    if engine is None:
        engine = g.engine = connect_to_database()
    return engine


@app.route("/")
def mapview():
    '''functions does an API call to dublin bikes and our RDS Database to plot icons and live data on google map displayed on page '''
      
    output = []    

    sql = """SELECT * FROM dublinbikes.stationStatic"""
             
    engine = get_db()
    rows = engine.execute(sql).fetchall()  # we use fetchall(), but probably there is only one station
    res = [dict(row.items()) for row in rows]  # use this formula to turn the rows into a list of dicts
    #data = jsonify(data=res)  # jsonify turns the objects into the correct respose
    data = json.dumps(res)
    parsedData = json.loads(data)

    #backup dublinbikes key (stephens: 88e5dc7b2582c68724462d2d858361ca93582780)
    #main dublinbikes key(daniels: c64916b14c557faa49fdf72b8902e4d9ff9afe35)
    r = requests.get("https://api.jcdecaux.com/vls/v1/stations?contract=Dublin&apiKey=88e5dc7b2582c68724462d2d858361ca93582780")
    data2 = r.json()
            
    #note there is no station 50 so need to split the ranges
    for i in range(1,101):
        if(i!=50):
            
            #note: using value of bike station numbers as value of each button...
            infobox = "Name: " + str(data2[i]["name"]) + "<br/>" + "Status: " + str(data2[i]["status"]) + "<br/>" + "Available Stands: " + str(data2[i]["available_bike_stands"]) + "</br>" +  "Available Bikes: " + str(data2[i]["available_bikes"]) + "</br>" + "Historic Info: " + '<input type="submit" name="' + str(data2[i]["number"]) + '" value="See More" onclick="showDiv(' + "'" + str(data2[i]["name"]) + "'" + "," + "'" +  str(data2[i]["number"]) + "'" + ')">'
            
            output.append(
                    {
                            'icon': icons.dots.blue,
                            'lat': parsedData[i]["position_lat"],
                            'lng':parsedData[i]["position_lng"],
                            'infobox': infobox
                    }
                )
            
        trdmap = Map(
            identifier="trdmap",
            lat=53.350140,
            lng=-6.266155,

	#potential extra feature - needs custom styling however...
	#see http://flaskgooglemaps.pythonanywhere.com/ clusterd map in view
	#cluster=True,
	#cluster_gridsize=10,

	#note: style for map is being declared here...
            style="height:600px;width:800px;margin-left:50px;margin-right:50px;color:black;float:left;display:inline-block",
               
            markers = output
    )

    station(data2[i]["number"])

    return render_template('index.html', trdmap=trdmap)


@app.route('/station/<int:station_id>')
def station(station_id):
    '''function executes SQL query to our database using a customizeable station number based on station_id value passed in'''
    
    sql = """SELECT available_bikes, available_bike_stands,last_update from stationDynamic where number = {}""".format(station_id)
    
    engine = get_db()
    rows = engine.execute(sql).fetchall()  # we use fetchall(), but probably there is only one station
    res = [dict(row.items()) for row in rows]  # use this formula to turn the rows into a list of dicts
    data = jsonify(data=res)  # jsonify turns the objects into the correct respose
    #print(data)
    #jdata = json.dumps(res)
    return data #.get_data(as_text=True) #returns as string?

if __name__ == "__main__":
  app.run()
