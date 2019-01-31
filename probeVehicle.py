#!/usr/bin/python
  
import glob
import pandas as pd
import numpy as np
import os
import sys
import time
import datetime
import mpu

# In millisecs (20 seconds between OBU messages)
MAX_DELAY = 20000

# Create a dataframe from Probe Vehicle Data (PVD)
df = pd.read_csv("Advanced_Messaging_Concept_Development__Probe_Vehicle_Data.csv")
df = df[np.isfinite(df['modeTransmission'])]
# Create a dataframe using the Road Side Unit (RSU) information
df_rsu = pd.read_csv("rsu.csv")
# Create a dataframe using the Vehicle/On-board Unit (OBU) information
df_obu = pd.read_csv("vehicleOBU.csv")

# Add new columns to the PVD table
df['communicationType'] = np.nan
df['timeTransmission'] = np.nan
#df['transmissionLine'] = np.nan
df['transmissionPointLat'] = np.nan
df['transmissionPointLon'] = np.nan
df['msecTransmission'] = np.nan
df['OBUpath'] = np.nan

# Add new columns to the OBU table for tracking movement
df_obu['prevtimeTransmission'] = 0
df_obu['prevLatitude'] = np.nan
df_obu['prevLongitude'] = np.nan

# List of OBU IDs
obu_list = df_obu.OBUid.tolist()

# Using the previous time of transmission to confirm same route
def is_OBU_prevlocation(obuid, time):
  idx = obu_list.index(obuid)
  if (df_obu.iloc[idx,5] == 0):
    print('OBU-{0} zero time'.format(str(obuid)));
    return False
  elif (time - df_obu.iloc[idx,5] > MAX_DELAY):
    print('OBU-{0} long time'.format(str(obuid)));
    return False
  else:
    return True
        
# Return the previous latitude & longitude reported by the OBU
def get_OBU_prevlocation(obuid):
  idx = obu_list.index(obuid)
  return (df_obu.iloc[idx,6], df_obu.iloc[idx,7])

# Set the previous location and time of message for each OBU
def set_OBU_prevlocation(obuid, time, latitude, longitude):
  idx = obu_list.index(obuid)
  df_obu.iloc[idx,5] = time
  df_obu.iloc[idx,6] = latitude
  df_obu.iloc[idx,7] = longitude

# List of RSUs
rsu_list = df_rsu.RSUid.tolist()

# Return the latitude of the RSU
def get_rsu_latitude(rsuid):
  idx = rsu_list.index(rsuid)
  return (df_rsu.iloc[idx,1])

# Return the longitude of the RSU
def get_rsu_longitude(rsuid):
  idx = rsu_list.index(rsuid)
  return (df_rsu.iloc[idx,2])
  
# As most of the communication is happening over the CELL, we assumed a cell tower 
# location in the Tyson’s corner for Lat/Lon coordinates instead of leaving it as NULL.
cell_longitude = "-77.222878"
cell_latitude = "38.917415"
  
"""
Populate the new columns in the PVD dataframe.
 - Set communicationType to 'RSU' or 'CELL' based on modeTransmission field (RSU id or 999999 for cellular).
 - Set timeTransmission to an OmniSci compatible TIMESTAMP format based on timeReceived column.
 - Set the transmissionPointLat & transmissionPointLon columns, and they will get imported as GEOMETRIC POINT during OmniSci table creation.
 - The latitude and longitude for the vehicle location are values in FLOAT, and we used the OmniSci  import feature to automatically convert it into GEOMETRIC POINT.
"""
for idx, download in df.iloc[0:].iterrows():
  if str(int(download['modeTransmission'])) == '999999':
    sys.stdout.write('TRYING '+str(idx)+'  '+str(int(download['modeTransmission']))+'\n')
    df.loc[idx, 'communicationType'] = "CELL"
    df.loc[idx, 'msecTransmission'] = df.loc[idx, 'timeReceived'] - df.loc[idx, 'timeMessageCreated']
    secs = int(df.loc[idx, 'timeReceived'] / 1000)
    df.loc[idx, 'timeTransmission'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(secs))
    df.loc[idx, 'transmissionPointLat'] = cell_latitude
    df.loc[idx, 'transmissionPointLon'] = cell_longitude
  else:
    sys.stdout.write('TRYING '+str(idx)+'  '+str(int(download['modeTransmission']))+'\n')
    df.loc[idx, 'communicationType'] = "RSU"
    df.loc[idx, 'timeMessageCreated'] = df.loc[idx, 'timeReceived']
    df.loc[idx, 'msecTransmission'] = 0
    secs = int(df.loc[idx, 'timeReceived'] / 1000)
    df.loc[idx, 'timeTransmission'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(secs))
    rsu_longitude = str(get_rsu_longitude(download['modeTransmission']))
    rsu_latitude = str(get_rsu_latitude(download['modeTransmission']))
    df.loc[idx, 'transmissionPointLat'] = rsu_latitude
    df.loc[idx, 'transmissionPointLon'] = rsu_longitude
  # Create a LINESTRING in OBUpath column based some assumptions that it is part 
  # of the same test route.
  if (is_OBU_prevlocation(df.loc[idx, 'OBUid'], df.loc[idx, 'timeMessageCreated'])):
    obu_latitude, obu_longitude = get_OBU_prevlocation(df.loc[idx, 'OBUid'])
    lat1 = df.loc[idx, 'Latitude'].astype(float) 
    lon1 = df.loc[idx, 'Longitude'].astype(float) 
    lat2 = float(obu_latitude)
    lon2 = float(obu_longitude)
    # Calculate Haversine distance in kilometers.
    dist = mpu.haversine_distance((lat1, lon1), (lat2, lon2))
    if (dist > 0.5):
      print('Too far', dist, ':', lon1, ' ', lat1, ', ', lon2, ' ', lat2)
      df.loc[idx, 'OBUpath'] = np.nan
    else:
      linestring = "LINESTRING(" + df.loc[idx, 'Longitude'].astype(str) +" " + df.loc[idx, 'Latitude'].astype(str) +", " +str(obu_longitude) +" " +str(obu_latitude) +")"
      df.loc[idx, 'OBUpath'] = linestring
  else:
    df.loc[idx, 'OBUpath'] = np.nan
  set_OBU_prevlocation(df.loc[idx, 'OBUid'], df.loc[idx, 'timeMessageCreated'], df.loc[idx, 'Latitude'].astype(str), df.loc[idx, 'Longitude'].astype(str))

    
df.to_csv("probeVehicle.csv", index=False)

