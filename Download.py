import requests
import os

os.mkdir("data")

def doDownload(name, url):
    print("Downloading {}".format(name))
    response = requests.get(url, stream=True)
    handle = open("data/" + name, "wb")
    for chunk in response.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
            handle.write(chunk)


doDownload("Building_Permits.csv", "https://data.sfgov.org/api/views/i98e-djp9/rows.csv?accessType=DOWNLOAD")