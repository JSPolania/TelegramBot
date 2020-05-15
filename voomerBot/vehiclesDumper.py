import voomerBot.toHire_model as toHire_model
import voomerBot.movo_model as movo_model
from voomerBot.utils import flatten
import codecs
import simplejson as json
import csv
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]-[%(levelname)s] %(message)s')

keys = ['id', 'latitude', 'longitude', 'reference_code', 'status',
    'total_percentage', 'trip_status', 'deviceType', 'created_at',
    'updated_at', 'booking_id', 'booking_status', 'booking_type',
    'booking_UserId', 'booking_created_at', 'online']

def get_vehicles(region):
    try:
        f = os.environ.get('CREDENTIALS')
        cfg = json.loads(f)
    except(Exception) as e:
        logging.error('Error opening cfg.json')
        sys.exit()

    if len([None for item in ['user', 'password'] if item not in cfg]) > 0:
        logging.error('No user or password supplied')
        sys.exit()

    try:
        regions = movo_model.get_regions()
        if regions[0] == 200:
            siteIds = {region['shortname']:region['siteid'] for region in regions[1]}
        else:
            logging.error('Couldn\'t load regions')
            sys.exit()
    except(Exception) as e:
        print(e)
        logging.error('Couldn\'t load regions2')
        sys.exit()

    def process(vehicles):
        return ({k:v for k,v in flatten(vehicle).items() if k in keys} for vehicle in vehicles)

    logging.info('Welcome to Vehicle Proc')

    token = toHire_model.login(cfg.get('user'), cfg.get('password'))
    if token[0] == 200:
        r = toHire_model.vehicle(token[1], siteIds.get(region))
        if r[0] == 200:
            return list(process(r[1]))
        else:
            logging.error('Vehicle HTTP status not OK')
    else:
        logging.error('Couldn\'t log-in')
    logging.info('Terminating Vehicle Proc')