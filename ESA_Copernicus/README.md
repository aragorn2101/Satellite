# Copernicus Dataspace download utility

This directory collects the scripts which can be used to prepare data queries and download data in batch from the Copernicus satellite data catalogues using APIs.

## OData_query_v1.1.py

The first step consists in querying the Copernicus database, through the OData interface, to obtain a list of available data files. The script takes a number of input parameters and constitutes a URL to query the database using the OData API. The input paramters must be set by the user in the preamble of the script. The following parameters are available in version 1.1 of the script.

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
On success, a query output is obtained from the API and is first translated to a JSON record on the fly. This record is then converted to a Pandas dataframe. The following columns of the output are filtered for our use:
```
'Id', 'Name', 'Checksum', 'Online', 'Downloaded'
```
As output, the script writes a log file which consists of a header listing the input parameters for the query, followed by a CSV table with the filtered data resulting from the query. The output log file is named according to the following format:
```
OData_{params_StartTime}_{params_StopTime}_query_{querying_Time}.log
```

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

