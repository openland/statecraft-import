import pandas as pd
import json
import requests
import time

print("Loading Permits...")
X = pd.read_csv("Building_Permits.csv", sep=',')
X = X[['Street Name', 'Street Suffix']]
X = X[~X.duplicated()]

print("Uploading Streets...")
s = requests.Session()
def doUpdateStreets(streets):
    start = time.time()
    domain = "sfhousing"
    staging_url = "https://statecraft-api-staging.herokuapp.com/api"
    production_url = "https://statecraft-api.herokuapp.com/api"
    local_url = "http://localhost:9000/api"
    
    headers = {'x-statecraft-domain': domain, 'Content-Type': 'application/json'}
    container = {
        "query": "mutation($args: [StreetInfo!]!) { updateStreets(streets: $args) }",
        "variables": {
            "args": streets
        }
    }
    data = json.dumps(container)
    r = s.post(staging_url, data=data, headers=headers, stream=False)
    print(r.text)
    end = time.time()
    print(end - start)

def uploadDataset(mapping, batch_size = 500):
    pending = []
    number = 0
    total_count = len(X)/batch_size
    for index, row in X.iterrows():
        p = {}
        for k in mapping.keys():
            if type(row[k]) is pd.Timestamp:
                p[mapping[k]] = row[k].strftime('%Y-%m-%d')
            elif row[k] != row[k]:
                # Nothing
                a = 1
            else:
                p[mapping[k]] = row[k]
        pending.append(p)
        if (len(pending) >= batch_size):
            number = number + 1
            print("Uploading {}/{}".format(number, total_count))
            doUpdateStreets(pending)
            pending = []
    if (len(pending)>0):
        doUpdateStreets(pending)

uploadDataset({ 'Street Name': 'name', 'Street Suffix': 'suffix' })