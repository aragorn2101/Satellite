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
import pandas as pd


###  BEGIN Parsing of command line arguments  ###
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
    print("The file ODATA_QUERY_LOG is a log file output by a OData_query.py script. The")
    print("basic format of this input file is CSV with a few lines for preamble where the")
    print("database query parameters are specified.")
    print()
    exit(1)
except FileNotFoundError:
    print("Cannot access {:s}!".format(argv[1]))
    print()
    exit(2)

###  END Parsing of command line arguments  ###


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


print()
exit(0)
