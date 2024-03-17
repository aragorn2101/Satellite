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
import json
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


###  BEGIN LOOP through records and download  ###
RecordIdx = 0
while (RecordIdx < log_df.shape[0]):
    # Check if file has already been downloaded, according to the log
    if (log_df.loc[RecordIdx, 'Downloaded'] == True):
        RecordIdx += 1

    else:
        # File name for data
        OutFile = log_df.loc[RecordIdx, 'Name'] + ".zip"

        print("\n------------------------------------------------------------------------------")
        print("#  Working on record with index {:3d}".format(RecordIdx))
        print("#  {:s}".format(OutFile))

        # Download if data is found online and hasn't been downloaded yet
        if (log_df.loc[RecordIdx, 'Online'] == True and log_df.loc[RecordIdx, 'Downloaded'] == False):

            # Build URL for data product
            url_data = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({:s})/$value".format( log_df.loc[RecordIdx, 'Id'] )

            # Build header using token
            hdrs = { "Authorization" : "Bearer {:s}".format(tkn_dict['access_token']) }

            # Open session and request data download
            session = requests.Session()
            session.headers.update( hdrs )
            session_res = session.get(url_data, headers=hdrs, stream=True)

            if (session_res.status_code == 200):  # everything ok

                print("\nDownloading ...")

                # Update log dataframe
                log_df.loc[RecordIdx, 'Downloaded'] = True
                RecordIdx += 1

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
