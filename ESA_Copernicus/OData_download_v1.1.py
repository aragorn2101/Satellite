#!/usr/bin/env python3
#
#  Script to query the databases of the Copernicus Dataspace and download
#  data automatically according to predefined parameters.
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
#  1.0: 02.03.2024
#       * Initial script.
#
#  1.1: 03.10.2024
#       * Loading token from file using the json library and parameterize the
#         query request URL.
#


from os.path import isfile
import json
import requests
import pandas as pd


#  Parameters defining data query
params_Collect = "SENTINEL-2"
params_Poly = "58.0586 -19.6394, 58.0586 -20.7519,57.06282 -20.7519,57.06282 -19.6394, 58.0586 -19.6394"
params_StartTime = "2024-02-21T00:00:00.000"
params_StopTime  = "2024-02-29T23:59:59.999"


#  File containing token as a JSON record
TokenFile = "CopernicusDataspace_token.json"

#  Open file and load into Python dictionary
try:
    with open(TokenFile) as f:
        tkn_dict = json.load(f)

except FileNotFoundError:
    print("Cannot access token file '{:s}'".format(TokenFile))



###  BEGIN Query Copernicus database  ###

#  Build URL for query
url_req  = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le 50.00) and Collection/Name eq '"
url_req += params_Collect
url_req += "' and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(("
url_req += params_Poly
url_req += "))') and ContentDate/Start gt "
url_req += params_StartTime
url_req += "Z and ContentDate/Start lt "
url_req += params_StopTime
url_req += "Z&$top=100"


#  Request
query_res = requests.get( url_req ).json()
query_df  = pd.DataFrame.from_dict(query_res['value'])

print()
print(query_df.info())
print()

for RecordIdx in range(query_df.shape[0]):
    print("{:s} {:s}".format(query_df['Id'].iloc[RecordIdx], query_df['Name'].iloc[RecordIdx]))


RecordIdx = 0
while (RecordIdx < query_df.shape[0]):
    OutFilename = query_df['Name'].iloc[RecordIdx] + ".zip"

    if isfile(OutFilename):  # avoid downloading same file twice
        RecordIdx += 1
    else:
        url_data = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({:s})/$value".format(query_df['Id'].iloc[RecordIdx])

        # Build header using token
        hdrs = { "Authorization" : "Bearer {:s}".format(tkn_dict['access_token']) }

        # Open session and request data download
        session = requests.Session()
        session.headers.update( hdrs )
        session_res = session.get(url_data, headers=hdrs, stream=True)


        if (session_res.status_code == 200):  # everything ok

            print("Downloading {:s}".format(OutFilename))

            """
            with open(OutFilename, 'wb') as outFile:
                for chunk in session_res.iter_content(chunk_size=8192):
                    if chunk:
                        outFile.write(chunk)
            """

        else:
            print("\nSession response status_code: {:d}".format(session_res.status_code))
            exit(3)


print()
exit(0)
