#!/usr/bin/env python3
#
#  Script to fetch a fresh token for the OData API of the
#  Copernicus Dataspace Ecosystem
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


#  Load libraries
from os.path import isfile
import shlex
import subprocess
import json


###  BEGIN Check if token file exists for output and construct cURL command  ###

#  Username and password of the Copernicus Dataspace account
Username = ""  ## PLEASE ADD USERNAME STRING
Password = ""  ## PLEASE ADD PASSWORD STRING

# Path for token file
TokenFile = "CopernicusDataspace_token.json"


if not isfile(TokenFile):
    print("Cannot access token file '{:s}'".format(TokenFile))
    exit(1)


#  Construct command for accessing <identity.dataspace.copernicus.eu>
#  to fetch token
print("Constructing command to access Copernicus Dataspace ...")
Copernicus_cmd = "curl -d 'client_id=cdse-public' -d 'username={0:s}' -d 'password={1:s}' -d 'grant_type=password' 'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token'".format(Username, Password)

###  END Check if token file exists for output and construct cURL command  ###



###  BEGIN Fetch token from Copernicus Dataspace  ###
try:
    print("\nRunning cURL command as subprocess ...")
    res = subprocess.run( shlex.split(Copernicus_cmd), capture_output=True )

    if (res.returncode == 6):
        raise ConnectionError

    # Extract output from subprocess' return object (CompletedProcess)
    print("\nDecoding CompletedProcess.stdout ...")
    stdout = json.loads( res.stdout.decode('utf-8') )

    if "error" in stdout.keys():
        raise RuntimeError
    else:
        # Print results
        print("res.returncode:")
        print(res.returncode)
        print()
        print("stdout:")
        print(stdout)
        print()
        print("res.stderr:")
        print(res.stderr)

        # Write JSON record to file
        with open(TokenFile, 'w') as f:
            json.dump(stdout, f)
            f.close()

    print("\nSuccessfully wrote token to file {:s}\n".format(TokenFile))
    exit(0)

except ConnectionError:
    print("\nError: could not resolve host: identity.dataspace.copernicus.eu\n")
    exit(2)
except RuntimeError:
    print("\nError: {:s}\n".format(stdout['error_description']))
    exit(3)
