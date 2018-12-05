#2
import csv

if __name__ == '__main__':
    volcano = open(r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r',encoding='utf8')  # open file up
    cpi = csv.reader(volcano)  # read opened file
   # print([item for item in cpi])
    for row in cpi:
        print(row)   # print rows in separate lines


#    There are 820 rows of data in this set with 16 columns. This dataset is about the history of volcanic eruptions
#    around the world. It comes from the National Geophysical Data Center database. A lot of the columns are
#    self-explanatory as to what they represent: such as Year, Month, Date of the volcanic occurence, Location, country,
#    latitude, longitude, and elevation represent where it occurred. The columns 'TSU' and 'EQ' tell if there was a
#    Tsunami or Earthquake associated with the eruption. The 'Type' Column is the type of volcano.
#    The 'Status' column gives the certainty of Holocene volcanism (historical meaning the highest certainty).
#    The 'Time' column is the date of the last known eruption D1 being 1964 or later, D2 (1900-1963), D3 (1800's),
#    D4 (1700's), D5 (15 or 1600's), D6 (1 AD - 1499 AD), D7 (BC), U (Undated, but probably holocene).
#    The 'VEI' column stands for Volcanic Explosivity Index from 0 (non-explosive, gentle) to 8 (colossal).
#    The 'Agent' column is what caused the fatalities in this occurrence:
#    A - Avalanche (debris/landslide), E (electrical), F (floods), G (gas), I (indirect deaths),
#    L - lava flows, M - mud flows, m - secondary mudflows, P - pyroclastic flows/direct blasts,
#    S - seismic, T - tephra (ash bombs, steam blasts), W - waves/tsunami
#    The quality of the data is good for the central columns describing year and location of eruption, but
#    Month, Day, and Agent have a lot of missing values. To resolve this, I will probably end up only using the
#    year of the eruption. I also may have to seperate out Agent into multiple columns
#    because some entries have multiple agents.

# 1 I will be using SQL Server as the database for my final project.
# 2 My database will be hosted using Azure.
# 3 I chose SQL Server because I like it's functionality and I have a lot of experience with it. I am using Azure
#    because it is easy to use and easy to connect with SQL Server. As well, I want to gain experience by using it.

import pyodbc
import csv

connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'
conn = pyodbc.connect(connection_string,autocommit=True)
curs = conn.cursor()

curs.execute(
    '''
    create table VolcanoData(
    Year nvarchar(5)
    ,Month int
    ,Day int 
    ,TSU varchar(4)
    ,EQ varchar(3)
    ,Name varchar(50)
    ,Location varchar(50) 
    ,Country varchar(50)
    ,Latitude float 
    ,Longitude float 
    ,Elevation float 
    ,Type varchar(50) 
    ,Status varchar(50) 
    ,Time varchar(50) 
    ,VEI varchar(50) 
    ,Agent varchar(50) 
        )

    '''
    )

if __name__ == '__main__':
    volcano = open (r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r')
    cpi = csv.reader(volcano)
    columns = next(volcano)
    query = 'insert into VolcanaData({0}) values ({1})'
    query = query.format(','.join(columns), ','.join('?' * len(columns)))
    cursor = conn.cursor()
    for data in volcano:
        cursor.execute(query, data)
        cursor.commit()

insert_query = 'INSERT INTO VolcanoData(Year,Month,Day,TSU,EQ,Name,Location,Country,Latitude,Longitude,Elevation,Type,Status,Time,VEI,Agent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
cpi_file = open(r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r',encoding='utf8')
cpi = csv.reader(cpi_file)
curs.executemany(insert_query, cpi)

setpk_query = 'ALTER TABLE VolcanoData ADD VolcanoID int identity (1,1) NOT NULL, CONSTRAINT VolcanoData_PK Primary Key(VolcanoID)'
curs.execute(setpk_query)

replacemonth = 'UPDATE VolcanoData SET Month = NULL WHERE Month = 0'
replaceday = 'UPDATE VolcanoData SET Day = NULL WHERE Day = 0'
curs.execute(replacemonth)
curs.execute(replaceday)

conn.commit()
curs.close()
conn.close()

import pyodbc
import csv

connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=**********;'
conn = pyodbc.connect(connection_string,autocommit=True)
curs = conn.cursor()


#if __name__ == '__main__':
  #  volcano = open (r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r')
  #  cpi = csv.reader(volcano)
   # columns = next(volcano)
    #query = 'insert into VolcanaData({0}) values ({1})'
    #query = query.format(','.join(columns), ','.join('?' * len(columns)))
    #cursor = conn.cursor()
    #for rows in volcano: # Iterate through csv
     #   curs.execute("INSERT INTO VolcanoData(Year,Month,Day,TSU,EQ,Name,Location,Country,Latitude,Longitude,Elevation,Type,Status,Time,VEI,Agent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",*rows)
      #  curs.commit()

replacemonth = 'UPDATE VolcanoData SET Month = NULL WHERE Month = 0'
replaceday = 'UPDATE VolcanoData SET Day = NULL WHERE Day = 0'
curs.execute(replacemonth)
curs.execute(replaceday)


conn.commit()
curs.close()
conn.close()



import pyodbc
from flask import Flask, g, render_template, abort, request
import json

connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'

#setup flask
app = Flask(__name__)
app.config.from_object(__name__)

#Before/Teardown
@app.before_request
def before_request():
    try:
        g.sql_conn =  pyodbc.connect(connection_string, autocommit=True)
    except Exception:
        abort(500, "No database connection could be established.")

@app.teardown_request
def teardown_request(exception):
    try:
        g.sql_conn.close()
    except AttributeError:
        pass

# Get all cpi
@app.route('/volcano', methods=['GET'])
def get_cpi_data():
    curs = g.sql_conn.cursor()
    query = 'select TOP 1000 from VolcanoData '
    curs.execute(query)

    columns = [column[0] for column in curs.description]
    data = []

    for row in curs.fetchall():
        data.append(dict(zip(columns, row)))
    return json.dumps(data, indent=4, sort_keys=True, default=str)

#get single data point
@app.route('/volcano/<string:VolcanoID>', methods=['GET'])
def get_single_cpi_data(VolcanoID):
    curs = g.sql_conn.cursor()
    curs.execute("select * from VolcanoData where VolcanoID = ?", VolcanoID)

    columns = [column[0] for column in curs.description]
    data = []

    for row in curs.fetchall():
        data.append(dict(zip(columns, row)))

    return json.dumps(data, indent=4, sort_keys=True, default=str)


@app.route('/volcano/<string:VolcanoID>', methods=['DELETE'])
def delete_single(VolcanoID):
    curs = g.sql.conn.cursor()
    curs.execute("delete from VolcanoData where VolcanoID = ?", VolcanoID)

    curs.commit()

@app.route('/volcano/<string:VolcanoID>', methods=['POST'])
def add_single(VolcanoID):
    curs = g.sql.conn.cursor()
    curs.execute("INSERT INTO VolcanoData(VolcanoID) Values(?)", VolcanoID)

    curs.commit()

    return 'success', 200


    return 'success', 200
if __name__ == '__main__':
    app.run(host="0.0.0.0")