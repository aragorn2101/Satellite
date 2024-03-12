#!/usr/bin/env python3
#
#  Script to query the databases of the Copernicus Dataspace and download
#  data automatically according to predefined parameters.
#  Version 2.0
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
#  1.1: 03.03.2024
#       * Loading token from file using the json library and parameterize the
#         query request URL.
#
#  2.0: 08.03.2024
#       * Refresh token on the fly during downloads.
#
#  2.1: 12.03.2024
#       * Check MD5 checksum after downloading.
#


from os.path import isfile
from time import sleep
import shlex
import json
import subprocess
import requests
from hashlib import md5
import pandas as pd


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
params_StartTime = "2024-02-21T00:00:00.000"
params_StopTime  = "2024-02-29T23:59:59.999"
params_Cloud = "50.00"
params_MaxRecords = "100"

###  END Set Data Query Parameters  ###


#  File containing token as a JSON record
TokenFile = "CopernicusDataspace_token.json"

#  Open file and load into Python dictionary
try:
    with open(TokenFile) as f:
        tkn_dict = json.load(f)

except FileNotFoundError:
    print("Cannot access token file '{:s}'".format(TokenFile))
    exit(1)



###  BEGIN Query Copernicus database  ###

#  Build URL for query
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

        print("\n------------------------------------------------------------------------------")
        print("#  Working on record with index  {:2d}".format(RecordIdx))
        print("#  {:s}".format(OutFilename))

        if (session_res.status_code == 200):  # everything ok

            with open(OutFilename, 'wb') as outFile:
                for chunk in session_res.iter_content(chunk_size=8192):
                    if chunk:
                        outFile.write(chunk)

            # Verify MD5 checksum and update log dataframe if everything fine
            print("Opening downloaded file from disk and verifying MD5 checksum ...")
            with open(OutFilename, 'rb') as f:
                rf = f.read()

                # Retrieve MD5 checksum from query records
                query_md5 = query_df.loc[i, ('Checksum')][0]['Value']

                # Calculate MD5 checksum using hashlib
                md5sum = md5(rf).hexdigest()
                print("MD5 checksum = {:s}".format(md5sum))

                # If MD5 do not match the one in the record, delete the bytes downloaded
                if (md5sum != query_md5):
                    print("\n***  Checksum does not match MD5 from query record!")
                    print("***  Error in downloading and/or writing file to disk!")
                else:
                    print("Checksum matches MD5 from query record.")

            RecordIdx += 1

        elif (session_res.status_code == 401):  # token expired

            ###  BEGIN Refresh token if expired  ###
            print("\nAccess token expired (response status code = 401)")
            print("Attempting to refresh the token ...")

            # Command for accessing <identity.dataspace.copernicus.eu>
            # for refreshing token
            Copernicus_cmd = "curl -d 'grant_type=refresh_token' -d 'refresh_token={:s}' -d 'client_id=cdse-public' 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'".format(tkn_dict['refresh_token'])

            # Print original command from Copernicus Dataspace website
            print("\nUsing the following command:")
            print(Copernicus_cmd)

            # Split command and run as subprocess to refresh token
            refresh_res = subprocess.run(shlex.split(Copernicus_cmd), capture_output=True)

            ##  Error resolving host website
            if (refresh_res.returncode == 6):
                print("\nError: could not resolve host <identity.dataspace.copernicus.eu>\n")
                exit(2)

            # Extract output from subprocess' return object (CompletedProcess)
            print("\nDecoding CompletedProcess.stdout from token request ...")
            stdout = json.loads( refresh_res.stdout.decode('utf-8') )

            ##  Error in refreshing token
            if "error" in stdout.keys():
                print("\nError: {:s}\n".format(stdout['error_description']))
                exit(3)

            # Write JSON record for token to file
            print("Writing JSON record for token to file {:s} ...".format(TokenFile))
            with open(TokenFile, 'w') as f:
                json.dump(stdout, f)

            ###  END Refresh token if expired  ###


            # Reload token into dictionary
            with open(TokenFile) as f:
                tkn_dict = json.load(f)

        elif (session_res.status_code == 429):  # Rate limiting
            print("Connection denied due to rate limiting (response status code = 429).")
            print("Will retry in 60 seconds ...")
            sleep(60)

        else:
            print("Session response status_code: {:d}".format(session_res.status_code))
            print("Session response reason: {:s}".format(session_res.reason))
            exit(2)



print()
exit(0)
