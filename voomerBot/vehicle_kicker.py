import voomerBot.toHire_model as toHire_model
import voomerBot.movo_model as movo_model
import codecs
import simplejson as json
import csv
import logging
import sys
import asyncio
import time
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]-[%(levelname)s] %(message)s')

def kicks_change_state(kicks_list, usr_action):

    region = 'bog'

    try:
        f = os.environ.get('MOVO_CREDENTIALS')
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
            logging.error('Couldn\'t load MOVO regions')
            sys.exit()
    except(Exception) as e:
        print(e)
        logging.error('Couldn\'t load MOVO regions2')
        sys.exit()

    token = toHire_model.login(cfg.get('user'), cfg.get('password'))
    
    #async def process_kicks(list_kicks):
    #    rsp = await asyncio.gather(*(toHire_model.action(token[1], usr_action, id, siteIds.get(region), 'kick') for id in list_kicks))
    #    return rsp
    
    logging.info(f"started at {time.strftime('%X')}")
    aiorsp = asyncio.run(toHire_model.action2(token[1], usr_action, kicks_list, siteIds.get(region), 'kick'))
    logging.info(f"finished at {time.strftime('%X')}")

    return aiorsp

def check_kicks(kicks):

    kicks_to_check = kicks[(kicks['zoho'] == 'Punto') & (kicks['ops_status'] == 'free')]

    ids = kicks_to_check.index.tolist()

    states = kicks_change_state(ids, 'stop')

    def get_info(msg):
        if msg['status'] == False:
            return 'Revisar, ' + msg['error']['type']
        else:
            return 'Ok' 

    labels = ['id', 'status_request', 'rsp']

    statesdf = pd.DataFrame(states, columns=labels)
    statesdf.set_index('id', inplace=True)

    checkeddf = pd.merge(left=kicks_to_check, 
                         right=statesdf, 
                         how='left',
                         left_index=True, 
                         right_index=True
    )

    checkeddf['msg'] = checkeddf.apply(lambda x: get_info(x['rsp']), axis=1)

    msg =''

    for _ ,row in checkeddf[checkeddf['msg'] != 'Ok'].iterrows():
        msg = msg + row['reference_code'] + ' ' + row['msg'] + '\n'

    return msg







