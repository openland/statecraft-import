import pandas as pd
import numpy as np
import requests
import json
import time
import math
import tools
import geojson
import shapely.wkt

print("Loading Parcels...")
PARCELS = pd.read_csv(
    "downloads/Parcels.csv",
    sep=',',
    dtype={
        'blklot': str,
        'geometry': str,
        'block_num': str,
        'lot_num': str,
        'mapblklot': str
    })

print("Uploading Parcels...")
print("Count {}".format(len(PARCELS)))
PARCELS = PARCELS[PARCELS['geometry'].notnull()]
print("Count {}".format(len(PARCELS)))
PARCELS = PARCELS[PARCELS['blklot'] == PARCELS['mapblklot']]
print("Count {}".format(len(PARCELS)))

def upload_batch(batch: tools.BatchBuilder):
    while batch.next_record():
        block_num = batch.read_string('block_num')
        lot_num = batch.read_string('lot_num')

        geo = batch.read_string('geometry')
        geo_converted = []
        if geo is not None:
            g1 = shapely.wkt.loads(geo)
            g2 = geojson.Feature(geometry=g1, properties={})
            geo = g2.geometry['coordinates']
            for coordinate in geo:
                arr = []
                if len(coordinate) < 50:
                    for coordinate2 in coordinate:
                        arr.append({
                            'latitude': coordinate2[1],
                            'longitude': coordinate2[0]
                        })
                geo_converted.append(arr)

        batch.copy_string('block_num', 'blockId')
        batch.copy_string('lot_num', 'lotId')
        batch.write_value('geometry', geo_converted)

    tools.upload_parcels(batch.data)


tools.batch_process(PARCELS, upload_batch, batch_size=50, max_workers=1)
