import pandas as pd
import numpy as np
import requests
import json
import time
import math
import tools
import geojson
import shapely.wkt
from shapely.ops import cascaded_union

# tools.SERVER = "local"

print("Loading Parcels...")
PARCELS = pd.read_csv(
    "downloads/Parcels.csv",
    sep=',',
    dtype={
        'blklot': str,
        'geometry': str,
        'block_num': str,
        'lot_num': str,
        'mapblklot': str,
        'datemap_dr': str
    })

print("Uploading Parcels...")
print("Count {}".format(len(PARCELS)))
PARCELS = PARCELS[PARCELS['geometry'].notnull()]
PARCELS = PARCELS[PARCELS['datemap_dr'].isnull()]
PARCELS = PARCELS[PARCELS['blklot'] != '0700035']
print("Count {}".format(len(PARCELS)))
PARCELS = PARCELS[PARCELS['blklot'] == PARCELS['mapblklot']]
print("Count {}".format(len(PARCELS)))

BLOCKS = {}

def upload_batch(batch: tools.BatchBuilder):
    while batch.next_record():
        block_num = batch.read_string('block_num')
        lot_num = batch.read_string('lot_num')

        geo = batch.read_string('geometry')
        geo_converted = []
        large = False
        if block_num not in BLOCKS:
            BLOCKS[block_num] = []
        if geo is not None:
            g1 = shapely.wkt.loads(geo).simplify(0.00001, preserve_topology=False)
            BLOCKS[block_num].append(g1)
            g2 = geojson.Feature(geometry=g1, properties={})
            geo = g2.geometry['coordinates']
            for coordinate in geo:
                arr = []
                if len(coordinate) > 100:
                    large = True
                for coordinate2 in coordinate:
                    arr.append({'la': coordinate2[1],'lo': coordinate2[0]})
                geo_converted.append(arr)

        
        if large:
            d = batch.reset_data()
            if len(d) > 0:
               tools.upload_parcels(d)

        batch.copy_string('block_num', 'blockId')
        batch.copy_string('lot_num', 'lotId')
        batch.write_value('geometry', geo_converted)

        if large:
            d = batch.reset_data()
            if len(d) > 0:
              tools.upload_parcels(d)

    tools.upload_parcels(batch.data)

tools.batch_process(PARCELS, upload_batch, batch_size=50, max_workers=1)

for key in BLOCKS.keys():
    united = cascaded_union(BLOCKS[key])
    simplified = united.simplify(0.00001, preserve_topology=False)
    converted = geojson.Feature(geometry=simplified, properties={}).geometry
    geo_converted = []
    print(converted.type)
    if converted.type == "MultiPolygon":
        for poly in converted.coordinates:
            for coordinate in poly:
                arr = []
                for coordinate2 in coordinate:
                    arr.append({ 'la' : coordinate2[1], 'lo' : coordinate2[0]})
                geo_converted.append(arr)
    if converted.type == "Polygon":
        for coordinate in converted.coordinates:
            arr = []
            for coordinate2 in coordinate:
                arr.append({ 'la' : coordinate2[1], 'lo' : coordinate2[0]})
            geo_converted.append(arr)

    tools.upload_blocks({'blockId': key, 'geometry': geo_converted})