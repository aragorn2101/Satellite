#!/usr/bin/env python3
#
#  Script to query the databases of the Copernicus Dataspace and output the
#  resulting records to a log file.
#  Version 1.1
#
#  Copyright (C) 2024  Nitish Ragoomundun, Mauritius
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# -----------------------------------------------------------------------------
#
#  Changelog:
#  1.1: 20.03.2024
#       * When MD5 is not available from query output a series of '-'.
#
#

###  BEGIN Set Data Query Parameters  ###

#  Input parameters for querying the database:
#
#  params_Collect : name of collection
#  params_Poly : coordinates of vertices constituting the polygon covering the
#                Area of Interest
#  params_StartTime : start date and time of sensing,
#                     format = "%Y-%m-%dT%H:%M:%S.000"
#  params_StopTime : end date and time of sensing,
#                     format = "%Y-%m-%dT%H:%M:%S.000"
#  params_Cloud : maximum percentage of cloud cover in image
#  params_MaxRecords : maximum number of records to retrieve from database
#                      matching the input parameters
#

##  Please set the following:

params_Collect = "SENTINEL-2"
params_Poly = "(58.0586 -19.6394, 58.0586 -20.7519,57.06282 -20.7519,57.06282 -19.6394, 58.0586 -19.6394)"
params_StartTime = "2021-08-01T00:00:00.000"
params_StopTime  = "2021-08-31T23:59:59.999"
params_Cloud = "50.00"
params_MaxRecords = "100"

###  END Set Data Query Parameters  ###


#
#  Output: after querying the database, the script writes a log file named with
#          the time interval defining the data search and the current date and
#          time (at the time the script is run). The log file contains a
#          preamble with the query parameters. The rest of the file is a set of
#          records listing the data files which match the input query
#          parameters. The preamble and the records are separated by the
#          following string: "---------------------"
#          The records are in a CSV format with the following header:
#          'Id', 'Name', 'Checksum', 'Online', 'Downloaded'
#


###  Libraries
from time import localtime, strptime, strftime
import requests
import pandas as pd



###  BEGIN Query Copernicus database and extract relevant data records ###

# Constitute URL for the query
url_req  = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le "
url_req += params_Cloud  # cloud cover
url_req += ") and Collection/Name eq '"
url_req += params_Collect  # colletion
url_req += "' and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON("
url_req += params_Poly  # coordinates for polygon
url_req += ")') and ContentDate/Start gt "
url_req += params_StartTime  # sensing time start
url_req += "Z and ContentDate/Start lt "
url_req += params_StopTime  # sensing time stop
url_req += "Z&$top="
url_req += params_MaxRecords

# Send request
query_res = requests.get( url_req ).json()
query_df  = pd.DataFrame.from_dict(query_res['value'])

print("\n------------------------------------------------------------------------------")
print("Output from query:\n")
print(query_df.info())
print("------------------------------------------------------------------------------")

###  END Query Copernicus database and extract relevant data records ###



###  BEGIN Construct output header and records for log file  ###

##  Make log file name
LogFile  = "OData_"
LogFile += strftime("%Y%m%d", strptime(params_StartTime, "%Y-%m-%dT%H:%M:%S.000"))
LogFile += "-"
LogFile += strftime("%Y%m%d", strptime(params_StopTime, "%Y-%m-%dT%H:%M:%S.999"))
LogFile += "_query_"
LogFile += strftime("%Y%m%d_%H%M%S", localtime())
LogFile += ".log"


##  Template for section preceding the CSV section
log_template = """\
Collection = {0:s}
Polygon = {1:s}
Sensing start = {2:s}
Sensing stop  = {3:s}
Cloud cover = {4:s}
Max records = {5:s}
---------------------
{6}"""


##  Make dataframe with records for output
log_df = query_df[['Id', 'Name', 'Checksum', 'Online']]


# Replace the original Checksum data with the value of the MD5 checksum only
for i in range(log_df.shape[0]):
    if (log_df.loc[i, ('Checksum')] == []):  # if MD5 not available
        log_df.loc[i, ('Checksum')] = "--------------------------------"
    elif (log_df.loc[i, ('Checksum')][0] == {}):
        log_df.loc[i, ('Checksum')] = "--------------------------------"
    else:
        log_df.loc[i, ('Checksum')] = log_df.loc[i, ('Checksum')][0]['Value']


# Insert new column at the end with the default value "False" for 'Downloaded'
log_df.insert(log_df.shape[1], 'Downloaded', False)


print("\n------------------------------------------------------------------------------")
print("Output to be written to file:\n")
print(log_df.info())
print("------------------------------------------------------------------------------")

###  END Construct output header and records for log file  ###



###  BEGIN Write output to file  ###

print("\nWriting query parameters and results to file {:s}".format(LogFile))
with open(LogFile, 'w') as f:
    f.write(log_template.format(params_Collect, params_Poly, params_StartTime, params_StopTime, params_Cloud, params_MaxRecords, log_df.to_csv(index=False)))

###  END Write output to file  ###


print()
exit(0)
