
# coding: utf-8

# # Volcano Dataset

# ### Display CSV in Seperate Rows

# In[91]:


import pandas as pd
import csv

volcano = open(r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r',encoding='utf8') #open file
cpi = csv.reader(volcano) #read file

#print((item for item in cpi))
for row in cpi:
    print(row) #print rows in seperate lines


# ### Explanation of the Dataset

# There are 820 rows of data in this set with 16 columns. This dataset is about the history of volcanic eruptions around the world. It comes from the National Geophysical Data Center database. A lot of the columns are self-explanatory as to what they represent: such as Year, Month, Date of the volcanic occurence, Location, country,latitude, longitude, and elevation represent where it occurred. The columns 'TSU' and 'EQ' tell if there was a Tsunami or Earthquake associated with the eruption. The 'Type' Column is the type of volcano. The 'Status' column gives the certainty of Holocene volcanism (historical meaning the highest certainty). The 'Time' column is the date of the last known eruption D1 being 1964 or later, D2 (1900-1963), D3 (1800's),
#  D4 (1700's), D5 (15 or 1600's), D6 (1 AD - 1499 AD), D7 (BC), U (Undated, but probably holocene). The 'VEI' column stands for Volcanic Explosivity Index from 0 (non-explosive, gentle) to 8 (colossal). The 'Agent' column is what caused the fatalities in this occurrence: A - Avalanche (debris/landslide), E (electrical), F (floods), G (gas), I (indirect deaths), L - lava flows, M - mud flows, m - secondary mudflows, P - pyroclastic flows/direct blasts, S - seismic, T - tephra (ash bombs, steam blasts), W - waves/tsunami. The quality of the data is good for the central columns describing year and location of eruption, but Month, Day, and Agent have a lot of missing values. To resolve this, I will probably end up only using the year of the eruption. I also may have to seperate out Agent into multiple columns because some entries have multiple agents.

# ### Connect to Microsoft Azure

# In[25]:


import pyodbc
import csv

connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'
conn = pyodbc.connect(connection_string,autocommit=True)
curs = conn.cursor()


# ### Create Table in SQL

# In[ ]:


curs.execute(
    '''
    create table VolcanoDataset(
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


# In[15]:


volcano = open (r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r')
cpi = csv.reader(volcano)
columns = next(volcano)
query = 'insert into VolcanaData({0}) values ({1})'
query = query.format(','.join(columns), ','.join('?' * len(columns)))
cursor = conn.cursor()
#for data in volcano:
 #   cursor.execute(query, data)
  #  cursor.commit()
        


# ### Import data into VolcanoDataset Table

# In[14]:


insert_query = 'INSERT INTO VolcanoDataset(Year,Month,Day,TSU,EQ,Name,Location,Country,Latitude,Longitude,Elevation,Type,Status,Time,VEI,Agent) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
cpi_file = open(r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r',encoding='utf8')
cpi = csv.reader(cpi_file)
curs.executemany(insert_query, cpi)


# #### Add Unique Identifier

# In[17]:


setpk_query = 'ALTER TABLE VolcanoDataset ADD VolcanoID int identity (1,1) NOT NULL, CONSTRAINT VolcanoDataset_PK Primary Key(VolcanoID)'
curs.execute(setpk_query)


# #### Alter Month and Day to include NULL

# In[26]:


replacemonth = 'UPDATE VolcanoDataset SET Month = NULL WHERE Month = 0'
replaceday = 'UPDATE VolcanoDataset SET Day = NULL WHERE Day = 0'
curs.execute(replacemonth)
curs.execute(replaceday)

conn.commit()


# #### Remove Month and Day Columns

# In[28]:


removemonth = 'ALTER TABLE VolcanoDataset DROP COLUMN Month'
removeday = 'ALTER TABLE VolcanoDataset DROP COLUMN Day'
curs.execute(removemonth)
curs.execute(removeday)

conn.commit()


# ### Display First 100 Rows

# In[29]:


connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'
conn = pyodbc.connect(connection_string,autocommit=True)

sql = '''
    Select TOP 100 *
    From VolcanoDataset
    '''
pd.read_sql_query(sql, conn)


# ## Displays countries with most volcanic eruptions

# In[32]:


connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'
conn = pyodbc.connect(connection_string,autocommit=True)

sql = '''
    Select distinct Country, Count(Country) as 'Number of Appearances'
    From VolcanoDataset
    Group by Country
    Order by Count(Country) DESC;
    '''
pd.read_sql_query(sql, conn)


# ##### Download and Enable Mapping Package

# !pip install ipyleaflet

# !jupyter nbextension enable --py --sys-prefix ipyleaflet

# ### This produced a map when ran on an individual Jupyter Notebook... Tried to run with different packages to plot all points on a graph with python script.

# from ipyleaflet import (Map,DrawControl)
# volcanomap = Map(center=[-0.7893, 113.9213],zoom=5)
# dc=DrawControl(circle={'shapeOptions':{'color':'#ff9900'}})
# volcanomap.add_control(dc)
# volcanomap

# %matplotlib inline
# import numpy as np
# import matplotlib.pyplot as plt
# from mpl_toolkits.basemap import Basemap
# 
# m = Basemap(projection='robin', lon_0=0.)
# 
# fig = plt.figure(figsize=(20,10))
# fig.patch.set_facecolor('#e6e8ec')
# m.drawmapboundary(color='none', fill_color='white');
# 
# m.drawcoastlines(color='white')
# m.fillcontinents(color='black', lake_color='white')
# m.drawcountries(linewidth=1, color='white');
# 
# graticule_wid
# 

# ### Switch to using Google Maps

# In[80]:


import gmaps
import gmaps.datasets
import gmplot
import pandas as pd
import googlemaps

#volcano = open (r'C:/Users/maver/documents/volcano-dataset-whole.csv', 'r')

API_KEY = 'AIzaSyDGkgVbpUzKaV9nW0GA3qGN2shX8DtrSKM'
gm = googlemaps.Client(key=API_KEY)
gmaps.configure(api_key=API_KEY)

df=pd.read_csv('volcano-dataset-whole.csv', low_memory=False, index_col = 'key')
locations = df[['Latitude', 'Longitude']]
val = df['VEI']
df.head()

#volcanos = gmaps.dataset.load_dataset_as_df(volcano)
#fig = gmaps.figure(map_type='HYBRID')
#heatmap_layer = gmaps.heatmap_layer(volcanos)
#fig.add_layer(heatmap_layer)
#fig


# !pip install googlemaps

# #### Change VEI column to Float datatype

# In[68]:


changeVEIdata = 'ALTER TABLE VolcanoDataset ALTER COlUMN VEI float'
curs.execute(changeVEIdata)

conn.commit()


# ## Use Heatmap on Google Maps to plot where eruptions occured

# In[81]:


import gmaps
import gmaps.datasets
import gmplot
import pandas as pd
import googlemaps

API_KEY = 'AIzaSyDGkgVbpUzKaV9nW0GA3qGN2shX8DtrSKM'
gm = googlemaps.Client(key=API_KEY)
gmaps.configure(api_key=API_KEY)

connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'
conn = pyodbc.connect(connection_string,autocommit=True)

sql = '''
    Select *
    From VolcanoDataset;
    '''
df = pd.read_sql_query(sql, conn)
locations = df[['Latitude','Longitude']]
val = df['VEI']
df.head()


# In[82]:


def drawHeatMap(location, val, zoom, intensity, radius):
    heatmap_layer = gmaps.heatmap_layer(locations, val, dissipating = True)
    heatmap_layer.max_intensity = intensity
    heatmap_layer.point_radius = radius
    
    fig = gmaps.figure(map_type='HYBRID')
    fig.add_layer(heatmap_layer)
    return fig


# In[88]:


zoom=2
intensity=5
radius=150

drawHeatMap(locations, val, zoom, intensity, radius)


# ## Function ran, but no map appeared.. 3rd attempt with same result.

# ### Countries with worst eruptions

# In[93]:


connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'
conn = pyodbc.connect(connection_string,autocommit=True)

sql = '''
    Select distinct Country, VEI, COUNT(Country) as 'Number'
From VolcanoDataset
WHERE VEI = 6 or VEI = 7
Group by Country, VEI
Order by VEI Desc, COUNT(Country) DESC
    '''
pd.read_sql_query(sql, conn)


# ### Eruptions in US States (Other than Alaska and Hawaii)

# In[94]:



connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=mis5400project.database.windows.net,1433;Database=MIS5400Project;Uid=MavMonster@mis5400project;Pwd=Jm1bk32f*1;'
conn = pyodbc.connect(connection_string,autocommit=True)

sql = '''
    Select Name, Year, Location
From VolcanoDataset
Where Country = 'United States' AND Location LIKE 'US%'
Order by Year DESC
    '''
pd.read_sql_query(sql, conn)


# ## Connecting to Flask

# In[101]:


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
    
    curs.commit()
    
    return 'success', 200

