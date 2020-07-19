#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# In[3]:


import numpy as np
import pandas as pd


# In[4]:


import datetime as dt


#  Reflect Tables into SQLAlchemy ORM

# In[5]:


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


# In[13]:


database_path = r"C:\Users\jimkn\Downloads\Homework_10-Advanced-Data-Storage-and-Retrieval_Instructions_Resources_hawaii.sqlite"
engine = create_engine(f"sqlite:///{database_path}")


# In[14]:


# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# In[15]:


# We can view all of the classes that automap found
Base.classes.keys()


# In[16]:


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


# In[17]:


# Create our session (link) from Python to the DB
session = Session(engine)


# In[18]:



engine.execute('SELECT * FROM Measurement LIMIT 5').fetchall()


# In[19]:


engine.execute('SELECT * FROM Station LIMIT 5').fetchall()


#  Exploratory Climate Analysis

# In[25]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results
recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

recent_date = recent_date[0]
print(recent_date)
   
# Calculate the date 1 year ago from the last data point in the database

one_year_ago = dt.date(2017, 8, 23) - dt.timedelta(days=365)
print(one_year_ago)


# In[49]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results
last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

# Get latest date
last_date = max_date[0]

# Calculate the date one year ago from last date
year_ago = dt.datetime.strptime(max_date, "%Y-%m-%d") - dt.timedelta(days=365)

# Perform a query to retrieve the data and precipitation scores
query = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= year_ago).all()

# Save the query results as a Pandas DataFrame and set the index to the date column
precip_df = pd.DataFrame(query,columns=['date', 'precipitation'])

# Change to datetime object
precip_df['date'] = pd.to_datetime(precip_df['date'], format='%Y-%m-%d')

# Set index to date
precip_df.set_index('date', inplace=True)

# Sort the dataframe by date
precip_df = precip_df.sort_values(by='date',ascending=True)

# Use Pandas Plotting with Matplotlib to plot the data
precip_df .plot(title="Precipitation (12 months)")
plt.legend(loc='upper center')
plt.show()


# In[50]:


# Use Pandas to calcualte the summary statistics for the precipitation data
precip_df.describe()


# In[51]:


# Design a query to show how many stations are available in this dataset?
stations = session.query(Measurement).group_by(Measurement.station).count()
print("There are {} stations available.".format(stations))


# In[53]:


# What are the most active stations? (i.e. what stations have the most rows)?
# List the stations and the counts in descending order.

active_stations = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
active_stations


# In[59]:


# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature of the most active station?

most_active_station=active_stations[0][0]


temp = [func.min(Measurement.tobs),
       func.max(Measurement.tobs),
       func.avg(Measurement.tobs)]
session.query(*temp).filter(Measurement.station==most_active_station).all()


# In[60]:


# Choose the station with the highest number of temperature observations.

highest_number_temps = session.query(Measurement.station, func.count(Measurement.tobs)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
highest_number_temps= highest_number_temps[0]


# In[63]:


# Query the last 12 months of temperature observation data for this station and plot the results as a histogram

temp_observs = session.query(Measurement.tobs).filter(Measurement.date >= one_year_ago).filter(Measurement.station == highest_number_temps).all()
temp_observs = pd.DataFrame(temp_observs, columns=['temperature'])


# In[65]:


#Plot One Year Temperature Data for Most Active Station
temp_observs.plot.hist(bins=12, title="Temperature vs. Frequency Histogram")
plt.tight_layout()
plt.show()


#  Bonus Challenge Assignment

# In[66]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
   """TMIN, TAVG, and TMAX for a list of dates.
   
   Args:
       start_date (string): A date string in the format %Y-%m-%d
       end_date (string): A date string in the format %Y-%m-%d
       
   Returns:
       TMIN, TAVE, and TMAX
   """
   
   return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).       filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# function usage example
print(calc_temps('2012-02-28', '2012-03-05'))


# In[72]:


# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.
trip = calc_temps('2017-07-01','2017-07-10')
trip


# In[81]:


# Plot the results from your previous query as a bar chart. 
# Use "Trip Avg Temp" as your Title
# Use the average temperature for the y value
# Use the peak-to-peak (tmax-tmin) value as the y error bar (yerr)

trip_temp_df = pd.DataFrame(trip, columns=['tmin', 'tavg', 'tmax'])

trip_temp_df.plot.bar(y='tavg', yerr=(trip_temp_df['tmax'] - trip_temp_df['tmin']), title='Trip Avg Temp', color='maroon', alpha=0.5, figsize=(4,6))
plt.xticks(np.arange(1, 1.0))
plt.ylabel("Temp (F)")
plt.show()


# In[82]:


# Calculate the total amount of rainfall per weather station for your trip dates using the previous year's matching dates.
# Sort this in descending order by precipitation amount and list the station, name, latitude, longitude, and elevation


# In[83]:


# Create a query that will calculate the daily normals 
# (i.e. the averages for tmin, tmax, and tavg for all historic data matching a specific month and day)

def daily_normals(date):
   """Daily Normals.
   
   Args:
       date (str): A date string in the format '%m-%d'
       
   Returns:
       A list of tuples containing the daily normals, tmin, tavg, and tmax
   
   """
   
   sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
   return session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()
   
daily_normals("01-01")


# In[86]:


# calculate the daily normals for your trip
# push each tuple of calculations into a list called `normals`

# Set the start and end date of the trip
start_date = '2017-07-01'
end_date ='2017-07-10'

# Create date range
dates = session.query(Measurement.date).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).group_by(Measurement.date).all()


# Find the start month/day and the end month/day 
start_month_day = trip_dates_stripped[0]
end_month_day = trip_dates_stripped[-1]


#daily_normals = [session.query(func.min(Measurement.tobs),func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).group_by(Measurement.date).all()]
    
# Loop through the list of %m-%d strings and calculate the normals for each date    
daily_normals = [session.query(func.min(Measurement.tobs),
                       func.avg(Measurement.tobs),
                       func.max(Measurement.tobs)).filter(func.strftime("%m-%d", Measurement.date) >= start_month_day).filter(func.strftime("%m-%d", Measurement.date) <= end_month_day).group_by(func.strftime("%m-%d", Measurement.date)).all()]

daily_normals = daily_normals[0]
daily_normals


# In[90]:


# Load the previous query results into a Pandas DataFrame and add the `trip_dates` range as the `date` index
   
daily_normals_df= pd.DataFrame(daily_normals,columns=['tmin', 'tavg', 'tmax'])

# Add dates
daily_normals_df['date']= trip_dates

# Change datatype from element to datetime object
daily_normals_df['date'] = pd.to_datetime(daily_normals_df['date'], format='%Y-%m-%d')

# Set index to date
daily_normals_df.set_index('date',inplace=True)

daily_normals_df  


# In[94]:


# Plot the daily normals as an area plot with `stacked=False`
daily_normals_df.plot(kind='area', alpha=.2, stacked=False, x_compat=True, title="Aggregate Daily Normals for Trip Dates")
plt.tight_layout()
plt.show()


# In[ ]:




