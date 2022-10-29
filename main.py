import json
import psycopg2 
from bs4 import BeautifulSoup
import requests

dbhost = 'localhost'
user_name = 'postgres'
user_password = 'admin'
dbdatabase = 'postgres'

connection = psycopg2.connect(host = dbhost, user = user_name, password = user_password, database = dbdatabase)

cur = connection.cursor()

try:
    cur.execute("""drop table test_data""")
    connection.commit()
except:
    pass

cur.execute("""create table if not exists test_data (DayOrder varchar(50), 
                            dayofweek varchar(100),
                            start_time varchar(100),
                            end_time varchar(100),
                            permit varchar(100),
                            location varchar(30),
                            optionalText varchar(250),
                            locationid varchar(50),
                            start24 varchar(100),
                            end24 varchar(100),
                            ccn varchar(50),
                            create_date varchar(50),
                            changed varchar(50),
                            block varchar(50),
                            lot varchar(50),
                            coldtruck varchar(1),
                            applicant varchar(100),
                            x varchar(30),
                            y varchar(30),
                            lattitude varchar(50),
                            longitude varchar(50),
                            location_2 varchar(300))""")



data_url = requests.get('https://data.sfgov.org/resource/jjew-r69b.json')

soup = BeautifulSoup(data_url.text, 'html.parser')


test_data = json.loads(data_url.text)[1]
for i in json.loads(data_url.text):
    try:
        dataRow = [
        i['dayorder'],
        i['dayofweekstr'],
        i['starttime'],
        i['endtime'],
        i['permit'],
        i['location'],
        i['optionaltext'],
        i['locationid'],
        i['start24'],
        i['end24'],
        i['cnn'],
        i['addr_date_create'],
        i['addr_date_modified'],
        i['block'],
        i['lot'],
        i['coldtruck'],
        i['applicant'],
        i['x'],
        i['y'],
        i['latitude'],
        i['longitude'],
        str(i['location_2'])
        ]

        cur.execute(
            """insert into test_data values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )""", dataRow 
        )
        connection.commit()
    except:
        pass


    
    
    



