import collections
import xml.etree.ElementTree as ET
import keytree
from shapely.geometry import shape, Point
import math
from functools import reduce
import requests
import simplejson as json
import os
from simplejson.errors import JSONDecodeError
from time import sleep
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from datetime import time, datetime
import pytz

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


#parseador de kmls para poligonos y puntos
def kmlparser(kml_string, shape_type='Polygon'):

    root = ET.fromstring(kml_string)
    ns = '{http://www.opengis.net/kml/2.2}'

    geofences = {}

    for placemark in root.iter(ns + 'Placemark'):
        #obtener el nombre de cada zona
        name = placemark.find(ns + 'name')
        shapes = placemark.find(ns + shape_type)
        geofences[name.text] = shape(keytree.geometry(shapes))
    return geofences

#definir si un punto esta en una determinada zona de operación
def get_zone(lat, lng, geozone):
    #trae la zona de operación segun los poligonos almacenados en geo zone
    coord = Point(lng, lat)
    for loc, pol in geozone.items():
        if pol.contains(coord):
            return loc
    return 'Sin zona'


#distancia en metros entre 2 coordenadas
def distance(lat1, lng1, lat2, lng2):
    def deg2rad(deg):
        return deg * math.pi / 180
    
    R = 6371
    dLat = deg2rad(lat2 - lat1)
    dLng = deg2rad(lng2 - lng1)
    
    a = math.pow(math.sin(dLat/2), 2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.pow(math.sin(dLng/2), 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))    
    d = R * c
    return d * 1000

def get_start(limit ,lat, lng, aliados, hot_spots=None):
    #compara la distancia entre el punto y todos los aliados y lo compara contra el limite
    #aliados es un diccionario nombre, punto aliado
    #hot_spots es un dicciomario nombre; poligono 
    #cada aliado en aliados es una lista con 2 elementos
    
    distances = [[name, distance(lat, lng, point.y, point.x), 'aliado'] for name, point in aliados.items()]
    
    if hot_spots is not None:
        for name, hotspot in hot_spots.items():
            distances.append([name, distance(lat, lng, hotspot.y, hotspot.x), 'hotzone'])

    closest = reduce(lambda a,b: a if a[1] < b[1] else b, distances)
    if closest[1] < limit:
        return closest[0], closest[2]
    else: 
        return 'en calle', 'en calle'


def zoho():
    
    app = os.environ.get('ZOHO_APP_NAME')
    view = os.environ.get('ZOHO_VIEW_NAME')
    parameters = json.loads(os.environ.get('ZOHO_CREDENTIALS'))
    network_retries = int(os.environ.get('NETWORK_RETRIES', '5'))
    network_sleep = int(os.environ.get('NETWORK_SLEEP', '2'))

    url = "https://creator.zoho.com/api/json/{}/view/{}".format(app, view)

    for i in range(network_retries):
        try:
            response = requests.get(url, params=parameters)
            if response.status_code == 200:
                keys = ['Ubicacion', 'Estado']
                kicks_list = response.json()['Inv_Scan_Scooter']
                kicks_dict = {i['QRScooter']: {k:v for k,v in i.items() if k in keys} for i in kicks_list}
                return [200,kicks_dict]
            else:
                return [response.status_code, 'zoho connection error']
        except (requests.exceptions.HTTPError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                UnicodeEncodeError, JSONDecodeError) as err:
            logging.warning('GET query error on zoho {} {}'.format(url, err))
            sleep(network_sleep**i)
    return [500, ['Internal Error']]

def distribution(city, active=True):
    """
    devuelve un dataframe con:
    index: zona, punto, lat, lng, dia (L-J, V-S, D)
    values: cantidad de patinetas por punto
    """

    city_src = {
        'mde': {'wkb': 'Distribución patinetas MDE', 'wks': 'Data MDE'},
        'bog': {'wkb': 'Distribución patinetas BOG V2', 'wks': 'Data BOG'},
    }

    scope = ['https://www.googleapis.com/auth/drive']
    credentials_dict = json.loads(os.environ.get('GOOGLE_API_CREDENTIALS'))
    network_retries = int(os.environ.get('NETWORK_RETRIES', '5'))
    network_sleep = int(os.environ.get('NETWORK_SLEEP', '2'))

    for i in range(network_retries):
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
            gc = gspread.authorize(credentials)
            wks = gc.open(city_src[city]['wkb']).worksheet(city_src[city]['wks'])
            data = wks.get_all_values()

            d = datetime.now().weekday()

            if d in [0,1,2]:
                d = 0
            elif d in [3,4]:
                d = 3
            elif d == 5:
                d = 6
            else:
                d  = 9

            h = datetime.now().hour
            if h in range(6,11):
                h = 1
            elif h in range(11, 16):
                h = 2
            else:
                h = 3

            reloc_row = d + h

            zonedata = [['Zona ' + r[1][0] + r[2], r[4], r[5], r[6], r[43 + reloc_row], r[19 + reloc_row]] for r in data[13:] if r[0] != '']

            labels = ['zona', 'punto', 'lat', 'lng', 'req', 'priority']

            pointsdf = pd.DataFrame(data=zonedata, 
                                    columns=labels,
                                    dtype=np.float64
            )
            
            pointsdf.set_index(['zona', 'punto'], inplace=True)
            pointsdf = pointsdf.infer_objects()

            data_dict = {}

            if active == True:
                data_dict['points'] = pointsdf[pointsdf['req'] > 0]
            else:
                data_dict['points'] = pointsdf    

            data_dict['map'] = kmlparser(data[11][4])   

            return data_dict    

        except (gspread.exceptions.GSpreadException,
                gspread.exceptions.APIError) as err:
            logging.warning('Google query error {}'.format(err))
            sleep(network_sleep**i)

def distribution_points(pointsfd):
    """
    pointsdf es un dataframe retornado por distribución
    Devuelve un diccionario con cada punto y Point class de shapely
    """
    points = {}
    for k, v in pointsfd[['lat','lng']].to_dict('index').items():
        aliado = k[1]
        points[aliado] = Point(v['lng'], v['lat'])
    return points