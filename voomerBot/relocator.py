from voomerBot.vehiclesDumper import get_vehicles
from voomerBot.utils import kmlparser, get_zone, get_start, zoho
import numpy as np
import pandas as pd
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import time, datetime
import pytz

def kicks_locations(kicks_list, points_list, zones, zoho_response):

    kicks_df = pd.DataFrame(data=kicks_list, dtype=np.float64)
    kicks_df.set_index('id',inplace=True)

    #change datetime to Colombia
    kicks_df['created_at'] = pd.to_datetime(kicks_df['created_at']).dt.tz_convert('America/Bogota')
    kicks_df['updated_at'] = pd.to_datetime(kicks_df['updated_at']).dt.tz_convert('America/Bogota')
    kicks_df['booking_created_at'] = pd.to_datetime(kicks_df['booking_created_at']).dt.tz_convert('America/Bogota')
    kicks_df['time_since_booking'] = datetime.now(pytz.timezone('America/Bogota')) - kicks_df['booking_created_at']

    #adjust reference_code
    kicks_df['reference_code'] = kicks_df['reference_code'].map(lambda x: str(x)[2:])
    
    kicks_df['zona'] = kicks_df.apply(lambda x: get_zone(x['latitude'], x['longitude'], zones), axis=1)
    kicks_df['punto'], kicks_df['pointType'] = zip(*kicks_df.apply(lambda x: get_start(65, x['latitude'], x['longitude'], points_list), axis=1))

    if zoho_response[0] == 200:
        zdict = zoho_response[1]

        def get_zoho_location(kick):
            if kick in zdict:
                return zdict[kick]['Ubicacion']
            else:
                return 'not in zoho'
        
        kicks_df['zoho'] = kicks_df.apply(lambda x: get_zoho_location(x['reference_code']), axis=1)
    else:
        kicks_df['zoho'] = 'zoho problem!'

    def ops_status(x):
        if x['online'] == 0:
            return 'offline'
        elif x['trip_status'] == 4:
            return 'free'
        elif x['trip_status'] == 1:
            return 'unavaliable'
        elif x['trip_status'] == 3:
            return 'parked'
        elif x['trip_status'] == 2:
            if x['booking_status'] == 1:
                return 'running_w_user'
            else:
                return 'running_wo_user'

    kicks_df['ops_status'] = kicks_df.apply(ops_status, axis=1)

    return kicks_df

def kicks_requirements(kicks, distribution):
    """
    kicks es un dataframe que viene de kicks_locations
    distribution es un dataframe de puntos de distribucion
    """
    kicks_in_operation = kicks[(kicks['zoho'] == 'Punto') & (kicks['status'] != 'unavailable') & (kicks['status'] != 'running')]
    streetdf = kicks_in_operation[(kicks_in_operation['punto'] == 'en calle')].sort_values(by='time_since_booking', ascending=False)

    kpp = kicks_in_operation.groupby(by='punto', observed=False).count()['reference_code']

    relocdf = pd.merge(left=kpp, 
                       right=distribution, 
                       how='right',
                       left_index=True, 
                       right_index=True
                       )

    relocdf.fillna(0, inplace=True)

    relocdf['Perc_Cap'] = relocdf['reference_code'] / relocdf['req']
    relocdf.sort_values(['priority', 'Perc_Cap'], ascending=[True, True], inplace=True)
    relocdf.sort_index(level=0, sort_remaining=False,inplace=True)

    def deviation(capacity, kicks):
        n = capacity - kicks
        if n > 0:
            return 'faltan %d' % n
        else:
            return 'sobran %d' % abs(n)
    
    def lineText(point, df):
        kicks = df['reference_code']
        cap = df['req']
        return '<b>{0}</b>\nCap: {1:.0f} - hay {2:.0f} 🛴 - {3}\n'.format(point, cap, kicks, deviation(int(cap), kicks))

    msg = []

    zones = sorted(relocdf.index.levels[0].unique())

    for zone in zones:

        #patinetas por punto
        points_text = '<b>🛴 por punto en {}</b>\n'.format(zone)
        resumen = relocdf.loc[zone].sum()
        points_text += lineText('TOTAL', resumen)

        for point ,row in relocdf.loc[zone].iterrows():
            points_text += lineText(point, row) 
        msg.append(points_text)
       
        #patinetas en calle
        kicks_on_street = streetdf[streetdf['zona'] == zone]['reference_code'].tolist()
        street_text = '<b>🛴 en calle en {0}: {1}</b>\n'.format(zone, len(kicks_on_street))
        street_text += '\n'.join(kicks_on_street)
        msg.append(street_text)
    
    return msg

def kicks_relocation(kicks, distribution):
    """
    kicks es un dataframe que viene de kicks_locations
    distribution es un dataframe de puntos de distribucion
    """
    kicks_in_operation = kicks[kicks['zoho'] == 'Punto']
    streetdf = kicks_in_operation[(kicks_in_operation['punto'] == 'en calle')].sort_values(by='time_since_booking', ascending=False)
    
    grouped = pd.pivot_table(
        kicks_in_operation, 
        values='reference_code',
        index=['punto'],
        columns=['ops_status'],
        aggfunc='count',
        fill_value=0
    )

    relocdf = pd.merge(left=grouped, 
                       right=distribution, 
                       how='right',
                       left_index=True, 
                       right_index=True
                       )

    relocdf.fillna(0, inplace=True)

    relocdf['Perc_Cap'] = relocdf['free'] / relocdf['req']
    relocdf.sort_values(['priority', 'Perc_Cap'], ascending=[True, True], inplace=True)
    relocdf.sort_index(level=0, sort_remaining=False,inplace=True)

    def lineText(point, serie):
        def _checker(colname, s):
            if colname in s:
                return s[colname]
            else:
                return 0
        return '<b>{0}</b>\nCap: {1:.0f}, F: {2:.0f}, P: {3:.0f}, R-: {4:.0f}, ND: {5:.0f}, O: {6:.0f}\n'.format(
            point, 
            _checker('req', serie), 
            _checker('free', serie),
            _checker('parked', serie),
            _checker('running_wo_user', serie),
            _checker('unavaliable', serie),
            _checker('offline', serie)
        )

    msg = []

    zones_partners = list(distribution.index.levels[0].unique())
    zones_op = list(kicks_in_operation['zona'].unique())
    zones = sorted(set(zones_partners + zones_op))

    for zone in zones:
        if zone in relocdf.index:
            #patinetas por punto
            points_text = '<b>🛴 por punto en {}</b>\n'.format(zone)
            resumen = relocdf.loc[zone].sum()
            points_text += lineText('TOTAL', resumen)

            for point ,row in relocdf.loc[zone].iterrows():
                points_text += lineText(point, row) 
            msg.append(points_text)

        #patinetas en calle
        kicks_on_street = streetdf[streetdf['zona'] == zone]['reference_code'].tolist()
        street_text = '<b>🛴 en calle {0}: {1}</b>\n'.format(zone, len(kicks_on_street))
        street_text += '\n'.join(kicks_on_street)
        msg.append(street_text)

    return msg


