#!/usr/bin/env python3
#
#  Script to download data from the Copernicus Dataspace Ecosystem
#  (https://dataspace.copernicus.eu/).
#  Version 3.0
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
#  3.0: 16.03.2024
#       * Separate query database and data download. From this version onwards,
#         the OData_query script must be run first. The latter outputs the
#         records to a log file and the OData_download script will only
#         download data by reading this log file.
#


from sys import argv
from os.path import isfile
import shlex
from time import sleep
import json
import subprocess
import requests
import pandas as pd


#  File containing token as a JSON record
TokenFile = "CopernicusDataspace_token.json"


###  BEGIN Parsing of command line arguments and load Token file  ###
try:
    # Check if there is an argument
    if len(argv) <= 1:
        raise OSError

    # Check if the argument points to a file
    if isfile(argv[1]):
        LogFile = argv[1]
    else:
        raise FileNotFoundError

except OSError:
    print("Usage: {:s} ODATA_QUERY_LOG".format(argv[0]))
    print("The file ODATA_QUERY_LOG is a log file output by the OData_query.py script.")
    print("The basic format of this input file is CSV with a few lines for preamble")
    print("where the database query parameters are specified.")
    print()
    exit(1)
except FileNotFoundError:
    print("Cannot access {:s}!".format(argv[1]))
    print()
    exit(2)

# Token file
try:
    with open(TokenFile) as f:
        tkn_dict = json.load(f)

    # Build header using token for session request
    hdrs = { "Authorization" : "Bearer {:s}".format(tkn_dict['access_token']) }


    # Command for accessing <identity.dataspace.copernicus.eu> in case token
    # needs refreshing
    Copernicus_cmd = "curl -d 'grant_type=refresh_token' -d 'refresh_token={:s}' -d 'client_id=cdse-public' 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'".format(tkn_dict['refresh_token'])

except FileNotFoundError:
    print("Cannot access token file '{:s}'".format(TokenFile))
    exit(3)

###  END Parsing of command line arguments and load Token file  ###


###  BEGIN Open log file, parse header and load records in dataframe  ###

#  Retrieve header
with open(LogFile) as f:
    count_hdr = 0
    line = ""
    log_hdr = ""
    while line != "---------------------\n":
        log_hdr += line
        line = f.readline()
        count_hdr += 1

#  Load records into dataframe
log_df = pd.read_csv(LogFile, skiprows=count_hdr)

###  END Open log file, parse header and load records in dataframe  ###


#  Template for writing preamble into log CSV file
log_template = """\
{0}---------------------
{1}"""


try:
    ###  BEGIN LOOP through records and download  ###
    RecordIdx = 0
    while (RecordIdx < log_df.shape[0]):
        # Check if file has already been downloaded, according to the log
        if (log_df.loc[RecordIdx, 'Downloaded'] == True):
            RecordIdx += 1

        else:
            # Output file name for data
            OutFile = log_df.loc[RecordIdx, 'Name'] + ".zip"

            print("\n------------------------------------------------------------------------------")
            print("#  Working on record with index {:3d}".format(RecordIdx))
            print("#  {:s}".format(log_df.loc[RecordIdx, 'Id']))
            print("#  {:s}".format(OutFile))

            # Download if data is found online and hasn't been downloaded yet
            if (log_df.loc[RecordIdx, 'Online'] == True and log_df.loc[RecordIdx, 'Downloaded'] == False):

                # Build URL for data product
                url_data = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({:s})/$value".format( log_df.loc[RecordIdx, 'Id'] )

                # Open session and request data download
                session = requests.Session()
                session.headers.update( hdrs )
                session_res = session.get(url_data, headers=hdrs, stream=True)

                if (session_res.status_code == 200):  # everything ok

                    print("\nDownloading ...")

                    with open(OutFile, 'wb') as f:
                        for chunk in session_res.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)

                    # Update log dataframe
                    log_df.loc[RecordIdx, 'Downloaded'] = True

                    RecordIdx += 1

                elif (session_res.status_code == 401):  # token expired

                    ###  BEGIN Refresh token if expired  ###

                    print("\nAccess token expired (response status code = 401)")
                    print("Attempting to refresh the token ...")

                    # Split command and run as subprocess to refresh token
                    refresh_res = subprocess.run(shlex.split(Copernicus_cmd), capture_output=True)

                    ##  Error resolving host website
                    if (refresh_res.returncode == 6):
                        print("\nError: could not resolve host <identity.dataspace.copernicus.eu>\n")
                        raise RuntimeError

                    # If ok, extract output from subprocess' return object (CompletedProcess)
                    print("Decoding CompletedProcess.stdout from token request ...")
                    stdout = json.loads( refresh_res.stdout.decode('utf-8') )

                    ##  Error in refreshing token
                    if "error" in stdout.keys():
                        print("\nError: {:s}\n".format(stdout['error_description']))
                        raise RuntimeError

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
                    print("\nSession response status_code: {:d}".format(session_res.status_code))
                    print("Session response reason: {:s}".format(session_res.reason))
                    RecordIdx += 1

            else:  # log_df['Online'] = False
                print("Data not found online!")
                RecordIdx += 1

    ###  END LOOP through records and download  ###

    print("\n------------------------------------------------------------------------------")
    print("# Updating log file {:s} ...".format(LogFile))
    with open(LogFile, 'w') as f:
        f.write(log_template.format(log_hdr, log_df.to_csv(index=False)))

    print()
    exit(0)

except RuntimeError:
    print("# Updating log file {:s} and exiting.".format(LogFile))
    with open(LogFile, 'w') as f:
        f.write(log_template.format(log_hdr, log_df.to_csv(index=False)))
    print()
    exit(4)
