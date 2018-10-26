import ujson as json
import urllib.parse
import urllib.request

from consts import COUCHDB_PASSWORD
from consts import COUCHDB_URL
from consts import COUCHDB_USERNAME

_opener_installed = False


def fetch(city, start, end, include_docs=True):
    global _opener_installed
    if not _opener_installed:
        # Prepare for the authentication
        manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        manager.add_password(None, COUCHDB_URL, COUCHDB_USERNAME, COUCHDB_PASSWORD)
        handler = urllib.request.HTTPBasicAuthHandler(manager)
        opener = urllib.request.build_opener(handler)
        opener.open(COUCHDB_URL)
        urllib.request.install_opener(opener)
        _opener_installed = True

    paras = {
        'start_key': [city, start.year, start.month, start.day],
        'end_key': [city, end.year, end.month, end.day],
        'reduce': 'false',
        'include_docs': 'true' if include_docs else 'false'}
    paras = urllib.parse.urlencode(paras).replace('%27', '%22')
    response = urllib.request.urlopen(COUCHDB_URL + '?' + paras)
    data = json.load(response)
    return data
