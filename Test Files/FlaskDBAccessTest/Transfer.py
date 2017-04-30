import json as simplejson
from flask import Flask, g, jsonify
from sqlalchemy import create_engine
import config

app = Flask(__name__)

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
    engine = getattr(g, 'engine', None)
    if engine is None:
        engine = g.engine = connect_to_database()
    return engine

@app.route("/available/<int:station_id>")
def get_stations(station_id):
    engine = get_db()
    data = []
    rows = engine.execute("SELECT available_bikes from stations where number = {};".format(station_id))
    for row in rows:
        data.append(dict(row))

    return json.dumps(available=data)

@app.route("/")
def main():
    return "Daniel is a great guy!"

@app.route('/station/<int:station_id>')
def station(station_id):
    # show the station with the given id, the id is an integer

    # this line would just return a simple string echoing the station_id
    # return 'Retrieving info for Station: {}'.format(station_id)

    # select the station info from the db
    sql = """
    select * from dublinbikes.availability where number = {}
    """.format(station_id)
    engine = get_db()
    rows = engine.execute(sql).fetchall()  # we use fetchall(), but probably there is only one station
    res = [dict(row.items()) for row in rows]  # use this formula to turn the rows into a list of dicts
    return jsonify(data=res)  # jsonify turns the objects into the correct respose

@app.route("/dbinfo")
def get_dbinfo():
    # this function shows the tables in your database
    sql = """
    SELECT table_name FROM information_schema.tables
    where table_schema='{}';
    """.format(config.DB)
    engine = get_db()
    rows = engine.execute(sql).fetchall()
    res = [dict(row.items()) for row in rows]
    print(res)
    return jsonify(data=res)

if __name__ == "__main__":
    """
    The URLs you should visit after starting the app:
    http://1270.0.0.1/
    http://1270.0.0.1/hello
    http://1270.0.0.1/user
    http://1270.0.0.1/dbinfo
    http://1270.0.0.1/station/42
    """
    app.run(debug=True)
