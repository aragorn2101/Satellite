#!/usr/bin/env python3
#
#  Script to download data from the Copernicus Dataspace Ecosystem
#  (https://dataspace.copernicus.eu/).
#  Version 3.1
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
#  3.1: 18.03.2024
#       * Handle errors and other issues as custom exceptions and ensure log
#         file is updated even if an unknown error occurs.
#
#
#  Usage: ./OData_download_vx.x.py INPUTLOGFILE
#
#  Exit status:
#      0      if OK,
#      1      no argument was passed on the command line,
#      2      cannot access log file passed to script,
#      3      cannot access file containing token,
#      4      could not refresh token on the fly,
#      5      session error while requesting download (session response status
#             code not in set: {200, 401, 429}),
#      6      error outside of exceptions handled in script.
#


#  Load libraries
from sys import argv
from os.path import isfile
import shlex
from time import sleep
import json
import subprocess
import requests
from hashlib import md5
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
    print("where the database query parameters are specified.\n")
    exit(1)
except FileNotFoundError:
    print("Cannot access {:s}!\n".format(argv[1]))
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


###  BEGIN Define custom exceptions  ###

class MD5SumError(Exception):
    pass

class TokenExpiredError(Exception):
    pass

class TokenRefreshError(Exception):
    pass

class RateLimitError(Exception):
    pass

class SessionError(Exception):
    pass

###  END Define custom exceptions  ###


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



###  BEGIN LOOP over records and download  ###
RecordIdx = 0
while (RecordIdx < log_df.shape[0]):
    try:
        # Check if file has already been downloaded, according to the log
        if (log_df.loc[RecordIdx, 'Downloaded'] == True):
            RecordIdx += 1

        ###  BEGIN ELSE 'Downloaded' = False  ###
        else:
            # Output file name for data
            OutFile = log_df.loc[RecordIdx, 'Name'] + ".zip"

            print("\n------------------------------------------------------------------------------")
            print("#  Working on record with index {:3d}".format(RecordIdx))
            print("#  {:s}".format(log_df.loc[RecordIdx, 'Id']))
            print("#  {:s}".format(OutFile))


            if (log_df.loc[RecordIdx, 'Online'] == False):
                print("\n***  NOTE: data not found online!")
                RecordIdx += 1


            ###  BEGIN ELSE 'Online' = True  ###
            # Download if data is found online and hasn't been downloaded yet
            else:

                # Build URL for data product
                url_data = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({:s})/$value".format( log_df.loc[RecordIdx, 'Id'] )

                # Open session and request data download
                session = requests.Session()
                session.headers.update( hdrs )
                session_res = session.get(url_data, headers=hdrs, stream=True)


                ##  If everything OK
                if (session_res.status_code == 200):

                    ###  BEGIN Download file and write bytes to file  ###

                    print("\nDownloading ...")

                    """
                    with open(OutFile, 'wb') as f:
                        for chunk in session_res.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    """

                    ###  END Download file and write bytes to file  ###


                    ###  BEGIN Verify download using MD5 checksum  ###

                    # If MD5 is not available, move on else, check
                    if (log_df.loc[RecordIdx, 'Checksum'] == "--------------------------------"):
                        print("**  Cannot verify data integrity since MD5 not available for this record.")
                        print("Marking as \'Downloaded\' and moving on.")
                        log_df.loc[RecordIdx, 'Downloaded'] = True
                        RecordIdx += 1
                    else:
                        print("Opening downloaded file from disk and verifying MD5 checksum ...")
                        with open(OutFile, 'rb') as f:
                            rf = f.read()
                            md5sum = md5(rf).hexdigest()
                            print("MD5 checksum = {:s}".format(md5sum))

                            # If MD5 do not match the one in the record, delete the bytes downloaded
                            if (md5sum != log_df.loc[RecordIdx, 'Checksum']):
                                raise MD5SumError

                            else:  # if everything OK
                                print("Checksum matches MD5 from query record. Updating log dataframe ...")
                                log_df.loc[RecordIdx, 'Downloaded'] = True
                                RecordIdx += 1

                    ###  END Verify download using MD5 checksum  ###

                elif (session_res.status_code == 401):  # token expired
                    raise TokenExpiredError

                elif (session_res.status_code == 429):  # Rate limiting
                    raise RateLimitError

                else:
                    raise SessionError

            ###  END ELSE 'Online' = True  ###

        ###  END ELSE 'Downloaded' = False  ###

    except MD5SumError:
        print("\n***  Checksum does not match MD5 from query record!")
        print("***  Error in downloading and/or writing file to disk!")
        print("***  Deleting downloaded data for this record and skipping it ...")

        rm_res = subprocess.run(['rm', OutFile])
        if (rm_res.returncode != 0):  # if rm command returns error
            print("\nrm {:s}".format(OutFile))
            print("Return code: {:d}".format(rm_res.returncode))

        RecordIdx += 1

    except TokenExpiredError:

        ###  BEGIN Refresh token if expired  ###
        try:

            print("\nAccess token expired (response status code = 401)")
            print("Attempting to refresh the token ...")

            # Split command and run as subprocess to refresh token
            refresh_res = subprocess.run(shlex.split(Copernicus_cmd), capture_output=True)

            ##  Error resolving host website
            if (refresh_res.returncode == 6):
                print("\n***  Error: could not resolve host <identity.dataspace.copernicus.eu>")
                print("***  Please fix the issue and re-run the script.")
                raise TokenRefreshError

            else:

                # If ok, extract output from subprocess' return object (CompletedProcess)
                print("Decoding CompletedProcess.stdout from token request ...")
                stdout = json.loads( refresh_res.stdout.decode('utf-8') )

                ##  Error in refreshing token
                if "error" in stdout.keys():
                    print("\n***  Error: {:s}".format(stdout['error_description']))
                    print("***  Please resolve the issue and re-run the script.")
                    raise TokenRefreshError

                else:  #  Success in refreshing token

                    # Write JSON record for token to file
                    print("Writing JSON record for token to file {:s} ...".format(TokenFile))
                    with open(TokenFile, 'w') as f:
                        json.dump(stdout, f)


                    ###  BEGIN Reload token into dictionary and re-initialize a few things  ###

                    with open(TokenFile) as f:
                        tkn_dict = json.load(f)

                    # Re-build header using token for session request
                    hdrs = { "Authorization" : "Bearer {:s}".format(tkn_dict['access_token']) }

                    # Command for accessing <identity.dataspace.copernicus.eu> in case token
                    # needs refreshing
                    Copernicus_cmd = "curl -d 'grant_type=refresh_token' -d 'refresh_token={:s}' -d 'client_id=cdse-public' 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'".format(tkn_dict['refresh_token'])

                    ###  END Reload token into dictionary and re-initialize a few things  ###

        except TokenRefreshError:
            print("\n# Updating log file {:s} and exiting.\n".format(LogFile))
            with open(LogFile, 'w') as f:
                f.write(log_template.format(log_hdr, log_df.to_csv(index=False)))
            exit(4)

        ###  END Refresh token if expired  ###


    except RateLimitError:
        print("Connection denied due to rate limiting (response status code = 429).")
        print("Will retry in 60 seconds ...")
        sleep(61)

    except SessionError:
        print("\nSession response status_code: {:d}".format(session_res.status_code))
        print("Session response reason: {:s}".format(session_res.reason))
        exit(5)


    ###  BEGIN Handle all exceptions and delete file if incomplete download  ###
    except:
        print("\n***  Unknown error or keyboard interrupt!")
        print("***  If there is a traceback output above, please fix the issue the re-run this")
        print("***  script.")

        # Check if file has been created already and proceed accordingly
        if isfile(OutFile):
            if (log_df.loc[RecordIdx, 'Checksum'] == "--------------------------------"):
                print("***  Cannot verify data integrity since MD5 not available for this record.")
                print("***  Since script was interrupted, as a precaution, we will be removing file")
                print("{:s} ...".format(OutFile))
                rm_res = subprocess.run(['rm', OutFile])
                if (rm_res.returncode != 0):  # if rm command returns error
                    print("\nrm {:s}".format(OutFile))
                    print("Return code: {:d}".format(rm_res.returncode))
            else:
                print("\nVerifying last download using MD5 checksum ...")
                with open(OutFile, 'rb') as f:
                    rf = f.read()
                    md5sum = md5(rf).hexdigest()
                    print("MD5 checksum = {:s}".format(md5sum))

                    # If MD5 do not match the one in the record, delete the bytes downloaded
                    if (md5sum != log_df.loc[RecordIdx, 'Checksum']):
                        print("\n***  Checksum does not match MD5 from query record!")
                        print("***  Incomplete download!")
                        print("***  Removing file {:s} ...".format(OutFile))
                        rm_res = subprocess.run(['rm', OutFile])
                        if (rm_res.returncode != 0):  # if rm command returns error
                            print("\nrm {:s}".format(OutFile))
                            print("Return code: {:d}".format(rm_res.returncode))
                    else:
                        print("MD5 checksum = {:s}".format(md5sum))
                        print("Checksum matches MD5 from query record. Updating log dataframe ...")
                        log_df.loc[RecordIdx, 'Downloaded'] = True
                        RecordIdx += 1


            print("\n# Updating log file {:s} and exiting.\n".format(LogFile))
            with open(LogFile, 'w') as f:
                f.write(log_template.format(log_hdr, log_df.to_csv(index=False)))

        exit(6)
    ###  END Handle all exceptions and delete file is incomplete download  ###


###  END LOOP over records and download  ###


print("\n------------------------------------------------------------------------------")
print("# Downloads complete.")
print("# Updating log file {:s} and exiting.\n".format(LogFile))
with open(LogFile, 'w') as f:
    f.write(log_template.format(log_hdr, log_df.to_csv(index=False)))

exit(0)
