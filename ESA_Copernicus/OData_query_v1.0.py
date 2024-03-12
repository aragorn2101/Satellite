#!/usr/bin/env python3
#
#  Script to query the databases of the Copernicus Dataspace and output the
#  resulting records to a log file.
#  Version 1.0
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
params_StartTime = "2024-03-07T00:00:00.000"
params_StopTime  = "2024-03-09T23:59:59.999"
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



print()
exit(0)
