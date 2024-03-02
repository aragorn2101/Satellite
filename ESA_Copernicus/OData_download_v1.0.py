#!/usr/bin/env python3
#
#  Query by
#  - Attributes: cloud cover
#  - Collection name
#  - Polygon
#  - Date of sensing
#
#  and download
#

import requests
import pandas as pd



ACCESS_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJYVUh3VWZKaHVDVWo0X3k4ZF8xM0hxWXBYMFdwdDd2anhob2FPLUxzREZFIn0.eyJleHAiOjE3MDkzNzQ2NDIsImlhdCI6MTcwOTM3NDA0MiwianRpIjoiZmIwOGE4MjAtZDFmMC00OWU0LTgxMWQtNjk1ZGViNGE5NWM5IiwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS5kYXRhc3BhY2UuY29wZXJuaWN1cy5ldS9hdXRoL3JlYWxtcy9DRFNFIiwiYXVkIjpbIkNMT1VERkVSUk9fUFVCTElDIiwiYWNjb3VudCJdLCJzdWIiOiIwMWM3MGY3ZS1mODU3LTQyYTYtYTIxNC1jNjFiZTMxMGY0NTkiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJjZHNlLXB1YmxpYyIsInNlc3Npb25fc3RhdGUiOiI1MmMyNGMyYi0zNjk1LTQ5NmUtYWI1Zi0zMzk4M2I5M2M3YjIiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cHM6Ly9sb2NhbGhvc3Q6NDIwMCIsIioiLCJodHRwczovL3dvcmtzcGFjZS5zdGFnaW5nLWNkc2UtZGF0YS1leHBsb3Jlci5hcHBzLnN0YWdpbmcuaW50cmEuY2xvdWRmZXJyby5jb20iXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iLCJkZWZhdWx0LXJvbGVzLWNkYXMiLCJjb3Blcm5pY3VzLWdlbmVyYWwiXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6IkFVRElFTkNFX1BVQkxJQyBvcGVuaWQgZW1haWwgcHJvZmlsZSBvbmRlbWFuZF9wcm9jZXNzaW5nIHVzZXItY29udGV4dCIsInNpZCI6IjUyYzI0YzJiLTM2OTUtNDk2ZS1hYjVmLTMzOTgzYjkzYzdiMiIsImdyb3VwX21lbWJlcnNoaXAiOlsiL2FjY2Vzc19ncm91cHMvdXNlcl90eXBvbG9neS9jb3Blcm5pY3VzX2dlbmVyYWwiLCIvb3JnYW5pemF0aW9ucy9kZWZhdWx0LTAxYzcwZjdlLWY4NTctNDJhNi1hMjE0LWM2MWJlMzEwZjQ1OS9yZWd1bGFyX3VzZXIiXSwiZW1haWxfdmVyaWZpZWQiOnRydWUsIm5hbWUiOiJOaXRpc2ggUmFnb29tdW5kdW4iLCJvcmdhbml6YXRpb25zIjpbImRlZmF1bHQtMDFjNzBmN2UtZjg1Ny00MmE2LWEyMTQtYzYxYmUzMTBmNDU5Il0sInVzZXJfY29udGV4dF9pZCI6Ijg4YzYyZWEyLTRmZGQtNDRiYS05NGZkLTMyYjc1MDNkODgxNyIsImNvbnRleHRfcm9sZXMiOnt9LCJjb250ZXh0X2dyb3VwcyI6WyIvYWNjZXNzX2dyb3Vwcy91c2VyX3R5cG9sb2d5L2NvcGVybmljdXNfZ2VuZXJhbC8iLCIvb3JnYW5pemF0aW9ucy9kZWZhdWx0LTAxYzcwZjdlLWY4NTctNDJhNi1hMjE0LWM2MWJlMzEwZjQ1OS9yZWd1bGFyX3VzZXIvIl0sInByZWZlcnJlZF91c2VybmFtZSI6Im4ucmFnb29tdW5kdW5AbXJpYy5tdSIsImdpdmVuX25hbWUiOiJOaXRpc2giLCJmYW1pbHlfbmFtZSI6IlJhZ29vbXVuZHVuIiwidXNlcl9jb250ZXh0IjoiZGVmYXVsdC0wMWM3MGY3ZS1mODU3LTQyYTYtYTIxNC1jNjFiZTMxMGY0NTkiLCJlbWFpbCI6Im4ucmFnb29tdW5kdW5AbXJpYy5tdSJ9.WNqFQ-Yv5HqIgOZ0DM-k7KT_5dTBtNrFddLBT6ye8fH-yvwiulwA3vksp6W1VHFWcUKz3cyL-N1ygjh1ialO26A2Lf5_kplSwUni1tIdGa4-2nLuDfegCbiAlfJ2u6M4U30ZP_qEdh0vtTwiOyEGNKxleyhJYbQLuPg-iRIHq0nYeDSFjz57obeyHsJO_J31OWa1F8iqvjF2EMo5gSzlkSQtX5FICRBK-TK_99Aa99sOcOcis6X2Woe2nwObOb6DK9Bklb-FQ0dYNoP-3Z5Blr-SCf0dP2QQFwt0PFcP7Zhlc6xZtw9bS4dIy3TnbB2uOJkVl8cGn4EKEJp9gOPMuA"


url_req = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le 50.00) and Collection/Name eq 'SENTINEL-2' and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON((58.0586 -19.6394, 58.0586 -20.7519,57.06282 -20.7519,57.06282 -19.6394, 58.0586 -19.6394))') and ContentDate/Start gt 2024-02-21T00:00:00.000Z and ContentDate/Start lt 2024-02-29T23:59:00.000Z&$top=100"



# Request
json = requests.get( url_req ).json()
df = pd.DataFrame.from_dict(json['value'])

print()
print(df.info())
print()

for i in range(df.shape[0]):
    print("{:s} {:s}".format(df['Id'].iloc[i], df['Name'].iloc[i]))



# Attempt to download the first file
OutputFilename = df['Name'].iloc[0] + ".zip"
url_data = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({:s})/$value".format(df['Id'].iloc[0])
hdrs = { "Authorization" : "Bearer {:s}".format(ACCESS_TOKEN) }

print("Downloading data file {:s}".format(OutputFilename))
print("from url:")
print("{:s}".format(url_data))
print("...")


session = requests.Session()
session.headers.update( hdrs )
response = session.get(url_data, headers=hdrs, stream=True)

print("response:")
print(response)


with open(OutputFile, 'wb') as file:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            file.write(chunk)

print()
exit(0)

