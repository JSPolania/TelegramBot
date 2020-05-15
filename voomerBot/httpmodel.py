import requests
import urllib3
import certifi
import simplejson as json
import ijson
from os import environ
from simplejson.errors import JSONDecodeError
from time import sleep
import logging
import asyncio
import aiohttp

network_retries = int(environ.get('NETWORK_RETRIES', '5'))
network_sleep = int(environ.get('NETWORK_SLEEP', '2'))
domain = environ.get('DOMAIN')
serviceid = environ.get('SERVICEID')
page_step = int(environ.get('PAGE_STEP', '5000'))
timeout_connect = float(environ.get('TIMEOUT_CONNECT', '9.15'))
timeout_read = float(environ.get('TIMEOUT_READ', '180'))

http = urllib3.PoolManager(maxsize=50, cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

def _make_headers(token=None, serviceid=serviceid):
    headers = {'Content-Type': 'application/json; charset=utf-8',
            'X-SERVICE-TOKEN': serviceid,
            'Connection':'close',
            'Accept-Encoding': 'gzip,deflate'}
    if token is not None:
        headers.update({'Authorization': 'Bearer %s' %(token)})
    return headers


def _get_query(url, token, payload=None):
    '''Non streamed HTTP GET query, requieres url, and OAuth token.
    Retries queries using exponential backoff'''
    for i in range(network_retries):
        try:
            r = requests.get(url, params=payload, timeout=(timeout_connect, timeout_read), headers=_make_headers(token))
            j = r.json()

            if r.status_code == 200:
                return [r.status_code, j['data']]
            else:
                return [r.status_code, j]
        except (requests.exceptions.HTTPError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                UnicodeEncodeError, JSONDecodeError) as err:
            logging.warning('GET query error {}'.format(url))
            if i != network_retries-1:
                sleep(network_sleep**i)
    return [500, ['Internal Error']]


def _stream_query(url, token, filter=None):
    '''Generator, yields first the HTTP status code and then the rest of the
    response payload. If the servers returns an HTTP error code the entire
    reponse is yielled at the second call.'''
    def _safe_next_item(iterator):
        '''Get next iterator element, safe enclosure'''
        while True:
            try:
                return next(iterator, None)
            except Exception as e:
                #urllib3.exceptions.IncompleteRead
                obj_type = _get_type(url)
                ERROR_IJSON.labels(obj_type).inc()
                logging.warning('Iterator error')
                continue
                #return e.partial

    def _chunked_query(step, filter):
        '''Generator, returns an HTTP IOStreams of each page.
        Retries queries using exponential backoff.
        Stream gets cut if excessive errors happen.'''
        index = 0
        while(True):
            for i in range(network_retries):
                try:
                    params = {'offset':index*step, 'limit':step,
                              'order':'[["id","ASC"]]'}
                    if filter is not None:
                        params.update(filter)
                    yield http.request('GET', url, fields=params,
                            timeout=urllib3.Timeout(connect=timeout_connect, read=timeout_read),
                            headers=_make_headers(token),
                            preload_content=False)
                    index += 1
                except (Exception) as e:
                    # Exceptions to set, don't kill me, I accept suggestions
                    logging.error('Chunked query error')
                    if i != network_retries-1:
                        sleep(network_sleep**i)

    cq = _chunked_query(page_step, filter)
    response = _safe_next_item(cq)
    yield response.status

    if response.status == 200:
        page = 0
        while response is not None:
            if response.status == 200:
                iterator = ijson.items(response, 'data.data.item')
                response = None
                index = 0
                try:
                    for item in iterator:
                        index += 1
                        yield item
                    if index == page_step:
                        response = _safe_next_item(cq)
                        page +=1
                except (ConnectionResetError, urllib3.exceptions.ProtocolError) as e:
                    logging.error('Connection reset error')
                    yield
                except (urllib3.exceptions.ReadTimeoutError) as e:
                    logging.error('Connection read timeout error')
                    yield
                except(ijson.common.IncompleteJSONError,  urllib3.exceptions.IncompleteRead):
                    logging.warning('Iterator error')
                    yield
            else:
                response = None
                yield
    else:
        yield response.data

def _get_streamed_query(url, token, payload=None):
    '''Streamed HTTP query, requieres url and OAuth token.
    Returns [HTTP Status, initialized StreamIO].'''
    sq = _stream_query(url, token, payload)
    status_code = next(sq, 0)
    try:
        if status_code == 200:
            return [200, sq]
        else:
            return [status_code, next(sq,'[]')]
    except (UnicodeEncodeError, JSONDecodeError):
        logging.warning('{} streamed query decoding error'.format(_get_type(url)))
    return [500, ['Internal Error']]


def _post_query(url, token=None, payload=None):
    '''Non streamed HTTP POST query, requieres url, payload and OAuth token.
    Retries queries using exponential backoff'''
    for i in range(network_retries):
        try:
            r = requests.post(url, json=payload, timeout=(timeout_connect, timeout_read), headers=_make_headers(token))
            j = r.json()

            if r.status_code == 200:
                return [r.status_code, j['data']]
            else:
                return [r.status_code, j]
        except (requests.exceptions.HTTPError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                UnicodeEncodeError, JSONDecodeError) as err:
            logging.warning('POST query error {}'.format(url))
            if i != network_retries-1:
                sleep(network_sleep**i)
    return [500, ['Internal Error']]


def _put_query(url, token=None, payload=None):
    '''Non streamed HTTP PUT query, requieres url, payload and OAuth token.
    Retries queries using exponential backoff'''
    for i in range(network_retries):
        try:
            r = requests.put(url, json=payload, timeout=(timeout_connect, timeout_read), headers=_make_headers(token))
            j = r.json()

            if r.status_code == 200:
                return [r.status_code, j['data']]
            else:
                return [r.status_code, j]
        except (requests.exceptions.HTTPError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                UnicodeEncodeError, JSONDecodeError) as err:
            logging.warning('PUT query error {}'.format(url))
            if i != network_retries-1:
                sleep(network_sleep**i)
    return [500, ['Internal Error']]


def login(user, password):
    r = _post_query('https://{}/v4/admin/login'.format(domain), payload={'username': user, 'password': password})
    if r[0] == 200:
        return [200, r[1]['token']['code']]
    else:
        return r


def vehicle(token, region=None):
    '''Fetch vehicles list'''
    def vehicle_filter(v):
        bookings = v.get('bookings')
        if isinstance(bookings, list):
            booking = dict([(x,bookings[0].get(x))
                        for x in ['id', 'status', 'type', 'created_at']] +
                    [('UserId', bookings[0].get('user', {}).get('id'))]) if len(bookings) > 0 else None
        else:
            booking = None

        return dict([(i, v.get(i))
                    for i in ['id', 'latitude', 'longitude', 'reference_code',
                        'status', 'total_percentage', 'trip_status', 'deviceType',
                        'created_at', 'updated_at', 'online']] +
                        [('booking', booking)])
    payload = dict((k,v) for k,v in (('site',region),) if v is not None)
    r = _get_streamed_query('https://{}/v4/admin/api/sharing/vehicle/'.format(domain), token, payload)
    if r[0] == 200:
        return [200, (vehicle_filter(v) for v in r[1])]
    else:
        return r

async def action(token, user_action, id, siteid, device = None):
    api = 'maintenance/sharing/'
    def get_cmd(user_action, device):
        return {
            'start': [['GET', api + device, 'start']],
            'stop': [['GET', api + device, 'stop']],
            'on_engine': [['GET', api + device, 'on']],
            'leave': [['GET', api + 'vehicle', 'leave']],
            'enable': [['GET', api + 'vehicle', 'available']],
            'disable': [['GET', api + 'vehicle', 'unavailable']]
            }.get(user_action)


    rst = []
    params = {k:v for k,v in {'site':siteid}.items() if v is not None}
    timeout = aiohttp.ClientTimeout(total=5*60, connect=timeout_connect, sock_connect=None, sock_read=None)

    try:
        cmds = get_cmd(user_action, device)   
        if cmds is not None:
            for cmd in cmds:
                url = 'https://core.2hire.io/v4/admin/api/%s/%s/%s' %(cmd[1], id, cmd[2])
                
                if cmd[0] == 'GET':
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, params=params, headers=_make_headers(token), timeout=timeout) as resp:
                            j = await resp.json()
                            rst.append([resp.status, j])
                            if resp.status != 200:
                                break
                else:
                    rst.append([500, {'error': 'No HTTP method'}])

            if not all([x[0] == 200 for x in rst]):
                return [rst[-1][0], id, [x[1] for x in rst]]
            else:
                return [200, id, json.dumps([x[1] for x in rst])]
        else:
            return [404, ['Command not found']]
    except (aiohttp.ClientConnectionError, UnicodeEncodeError, JSONDecodeError) as err:
        print(err, flush=True)




async def fetch_action(id, url, session, params, timeout):
    try:
        async with session.get(url) as resp:
            j = await resp.json()
            return [id, resp.status, j]
    except (aiohttp.ClientConnectionError, UnicodeEncodeError, JSONDecodeError) as err:
        print(err, flush=True)

async def action2(token, user_action, ids, siteid, device = None):
    api = 'maintenance/sharing/'
    def get_cmd(user_action, device):
        return {
            'start': ['GET', api + device, 'start'],
            'stop': ['GET', api + device, 'stop'],
            'on_engine': ['GET', api + device, 'on'],
            'leave': ['GET', api + 'vehicle', 'leave'],
            'enable': ['GET', api + 'vehicle', 'available'],
            'disable': ['GET', api + 'vehicle', 'unavailable']
            }.get(user_action)

    params = {k:v for k,v in {'site':siteid}.items() if v is not None}
    timeout = aiohttp.ClientTimeout(total=5*60, connect=timeout_connect, sock_connect=None, sock_read=None)

    tasks = []

    cmd = get_cmd(user_action, device) 
    async with aiohttp.ClientSession(headers=_make_headers(token)) as session:
        for id in ids:
            url = 'domain' %(cmd[1], id, cmd[2]) 
            task = asyncio.ensure_future(fetch_action(id, url, session, params, timeout))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
         
        return responses


  