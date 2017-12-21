import pandas as pd
import numpy as np
import requests
import json
import time
import math
import tools

print("Loading Permits...")
BUILDING_PERMITS = pd.read_csv(
    "downloads/Building_Permits.csv", sep=',', dtype={
        'Permit Number': str
    })

print("Uploading Permits...")

_permit_type_map = {}
_permit_type_map[1] = 'NEW_CONSTRUCTION'
_permit_type_map[2] = 'NEW_CONSTRUCTION'
_permit_type_map[3] = 'ADDITIONS_ALTERATIONS_REPARE'
_permit_type_map[4] = 'SIGN_ERRECT'
_permit_type_map[5] = 'GRADE_QUARRY_FILL_EXCAVATE'
_permit_type_map[6] = 'DEMOLITIONS'
_permit_type_map[7] = 'WALL_OR_PAINTED_SIGN'
_permit_type_map[8] = 'OTC_ADDITIONS'

_status_map = {}
_status_map['expired'] = 'EXPIRED'
_status_map['filed'] = 'FILED'
_status_map['issued'] = 'ISSUED'
_status_map['disapproved'] = 'DISAPPROVED'
_status_map['complete'] = 'COMPLETED'
_status_map['approved'] = 'APPROVED'
_status_map['withdrawn'] = 'WITHDRAWN'
_status_map['plancheck'] = 'PLANCHECK'
_status_map['filing'] = 'FILING'
_status_map['reinstated'] = 'REINSTATED'
_status_map['revoked'] = 'REVOKED'
_status_map['appeal'] = 'APPEAL'
_status_map['issuing'] = 'ISSUING'
_status_map['inspection'] = 'INSPECTING'
_status_map['upheld'] = 'UPHELD'
_status_map['incomplete'] = 'INCOMPLETE'
_status_map['granted'] = 'GRANTED'
_status_map['cancelled'] = 'CANCELLED'
_status_map['suspend'] = 'SUSPENDED'


def upload_batch(batch: tools.BatchBuilder):
    while batch.next_record():

        # Basic Info
        batch.copy_string('Permit Number', 'id')
        batch.copy_string('Description', 'description')
        batch.copy_string('Proposed Use', 'proposedUse')
        batch.copy_date('Permit Creation Date', 'createdAt')

        # Street Info
        streetName = batch.read_string('Street Name')
        streetNameSuffix = batch.read_string('Street Suffix')
        streetNumber = batch.read_int('Street Number')
        streetNumberSuffix = batch.read_string('Street Number Suffix')
        if streetNameSuffix == 'Ave':
            streetNameSuffix = 'Av'
        if streetNameSuffix == 'Blvd':
            streetNameSuffix = 'Bl'
        if streetNameSuffix == 'Ter':
            streetNameSuffix = 'Tr'
        if streetNameSuffix == 'Way':
            streetNameSuffix = 'Wy'
        if streetNameSuffix == 'Plz':
            streetNameSuffix = 'Pz'
        if streetNumber is not None and streetName is not None:
            street = {
                "streetName": streetName,
                "streetNameSuffix": streetNameSuffix,
                "streetNumber": streetNumber,
                "streetNumberSuffix": streetNumberSuffix
            }
            batch.write_value('street', street)

        # Statistics
        batch.copy_int('Number of Existing Stories', 'existingStories')
        batch.copy_int('Number of Proposed Stories', 'proposedStories')
        batch.copy_int('Existing Units', 'existingUnits')
        batch.copy_int('Proposed Units', 'proposedUnits')

        # Permit Type
        permit_type = batch.read_int('Permit Type')
        if permit_type in _permit_type_map:
            batch.write_string('type', _permit_type_map[permit_type])
            if permit_type == 1:
                batch.write_value('typeWood', False)
            elif permit_type == 2:
                batch.write_value('typeWood', True)
        else:
            print("Wrong Type: {}".format(permit_type))

        # Permit Status
        status = batch.read_string('Status')
        status_date = None
        # status_date = batch.read_date('Status Date')
        # print(type(status_date))
        if status in _status_map:
            batch.write_string('status', _status_map[status])
            batch.write_date('statusUpdatedAt', status_date)
            if (status == 'expired'):
                batch.write_date('expiredAt', status_date)
            elif (status == 'issued'):
                batch.write_date('issuedAt', status_date)
        else:
            print("Wrong Status: {}".format(status))

    tools.upload_permits(batch.data)


tools.batch_process(BUILDING_PERMITS, upload_batch, max_workers=1)
