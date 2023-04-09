import json
from datetime import datetime, timedelta
from geopy.distance import distance
from os import environ
from time import sleep
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, func
from sqlalchemy.exc import OperationalError

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate the distance between two points using the Haversine formula."""
    return distance((lat1, lon1), (lat2, lon2)).km

def get_last_hour_data(devices_table, psql_engine):
    # Create a connection object
    conn = psql_engine.connect()   

    # Get the last hour's data
    last_hour = datetime.now() - timedelta(hours=1)
    
    # the filter condition to get data for each device for each hour ()last hour
    data = conn.execute(devices_table.select().where(func.cast(devices_table.c.time, Integer) >= int(last_hour.timestamp())))
    
    # Close the connection
    conn.close()

    # Return the data
    return data

def aggregate_data(last_hour_data):
    """Aggregate the data for each device."""
    device_data = {}
    for row in last_hour_data:
        device_id = row[0]
        temperature = row[1]
        location = json.loads(row[2])
        latitude = float(location['latitude'])
        longitude = float(location['longitude'])
        time = row[3]

        # Calculate the distance moved by the device
        if device_id in device_data:
            previous_location = device_data[device_id]['location']
            distance_moved = calculate_distance(previous_location['latitude'], previous_location['longitude'], latitude, longitude)
        else:
            distance_moved = 0

        # Add the data to the device_data dictionary
        if device_id not in device_data:
            device_data[device_id] = {
                'max_temperature': temperature,
                'data_points': 1,
                'distance_moved': distance_moved,
                'location': {'latitude': latitude, 'longitude': longitude},
                'last_time': time
            }
        else:
            device_data[device_id]['max_temperature'] = max(device_data[device_id]['max_temperature'], temperature)
            device_data[device_id]['data_points'] += 1
            device_data[device_id]['distance_moved'] += distance_moved
            device_data[device_id]['location'] = {'latitude': latitude, 'longitude': longitude}
            device_data[device_id]['last_time'] = time
    return device_data


def store_aggregated_data(mysql_engine, device_data):
    """Store the aggregated data in the MySQL database."""
    metadata_mysql = MetaData()
    device_data_table = Table('device_data', metadata_mysql,
                              Column('id', Integer, primary_key=True),
                              Column('device_id', String(50), nullable=False),
                              Column('max_temperature', Integer, nullable=False),
                              Column('data_points', Integer, nullable=False),
                              Column('distance_moved', Integer, nullable=False),
                              Column('last_location', String(255), nullable=False),
                              Column('last_time', Integer, nullable=False))
    metadata_mysql.create_all(mysql_engine)
    with mysql_engine.connect() as conn:
        # Insert the aggregated data into the device_data table
        for device_id, data in device_data.items():
            print('Inserting data to MySQL: ',data)
            conn.execute(device_data_table.insert().values(
                device_id=device_id,
                max_temperature=data['max_temperature'],
                data_points=data['data_points'],
                distance_moved=data['distance_moved'],
                last_location=json.dumps(data['location']),
                last_time=data['last_time']
            ))
            
            
print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')

# Define the devices table
metadata_psql = MetaData()

metadata_psql.reflect(bind=psql_engine)

devices_table = metadata_psql.tables["devices"]

# Get last hour's data for each device
last_hour_data = get_last_hour_data(devices_table, psql_engine)

# Aggregate the data for each device
device_data = aggregate_data(last_hour_data)

# Store the aggregated data in the MySQL database
store_aggregated_data(mysql_engine, device_data)

print('ETL completed successfully')

