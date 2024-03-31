# Copernicus Dataspace download utility

This directory comprises scripts which can be used to prepare data queries and download data in batch from the Copernicus satellite data catalogues using APIs. The basic sequence of action is
1. Query the Copernicus database through the OData API interface using the `OData_query` script.
2. Fetch a fresh token to obtain clearance to initiate downloads through the OData API. This is done by running the `OData_fetch_token` script.
3. Launch download for a particular query by running the `OData_download` script.


## OData_query_v1.1.py

Querying the Copernicus database means probing the data repository and looking for data files corresponding to a set of parameters/characteristics based on our requirements in satellite data. This search is done through the OData API interface. The parameters are tuned in the preamble of the `OData_query` script. The following parameters are available in version 1.1 of the script:-

**params_Collect:** name of collection
**params_Poly:** coordinates of vertices constituting the polygon covering the Area of Interest
**params_StartTime:** start date and time of sensing, in format = `%Y-%m-%dT%H:%M:%S.000`
**params_StopTime:** end date and time of sensing, in format = `%Y-%m-%dT%H:%M:%S.000`
**params_Cloud:** maximum percentage of cloud cover in image
**params_MaxRecords:** maximum number of records to retrieve from database matching the input parameters

**Usage:**
```
./OData_query_v1.1.py
```
or
```
python OData_query_v1.1.py
```
On success, the list of files output by the query is first translated to a JSON record on the fly, which is then converted to a Pandas dataframe. The following columns of this dataframe are filtered for our use:
```
'Id', 'Name', 'Checksum', 'Online', 'Downloaded'
```
The final output of the script is a log file which consists of a header listing the input parameters for the query, followed by a CSV table generated from the abovementioned dataframe. The output log file is named according to the following format:
```
OData_{params_StartTime}_{params_StopTime}_query_{querying_Time}.log
```


## OData_fetch_token_v1.0.py
Prior to starting any data download through the OData API, we need to fetch an access token. This script takes as input the username and password of a user and request a token from the OData online interface. The user needs to set the username and password in the preamble of the script:
```
#  Username and password of the Copernicus Dataspace account
Username = ""  ## PLEASE ADD USERNAME STRING
Password = ""  ## PLEASE ADD PASSWORD STRING
```
The token is in the form of a JSON record which is written to a file called `CopernicusDataspace_token.json`.


## OData_download_v3.1.py

This script downloads data in batch. It takes as input the log file written by the `OData_query` script. Download sessions using the OData API are initiated using a token. This token is stored in a file called `CopernicusDataspace_token.json`, which is loaded at runtime. The download links are constructed using the file IDs stored in the log file. Once downloaded, the data integrity of every file is verified using the MD5 checksum. If everything is fine, the `'Downloaded'` column in the log dataframe is updated. The script handles many of the possible exceptions and in all cases updates the dataframe and writes it out to the log file before exiting.

**Usage:**
```
$ ./OData_download_v3.1.py INPUTLOGFILE
```
or
```
$ python OData_download_v3.1.py INPUTLOGFILE
```
**Exit status:**
```
      0      if OK
      1      no argument was given on the command line,
      2      cannot access log file passed to script,
      3      cannot access file containing token,
      4      could not refresh token on the fly,
      5      session error while requesting download (session response status
             code not in set {200, 401, 429}),
      6      error outside of exceptions defined in script.
```

