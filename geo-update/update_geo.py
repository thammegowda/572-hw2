import notes.util.solr as solr
import subprocess as sp
import json
from repoze.lru import lru_cache
import requests
import re

@lru_cache(maxsize=5000)
def get_gps_rest(name, service_url="http://localhost:8765/api/search"):
    '''
    Gets GPS of a location
    :param name: location name
    :return: (latitude, longitude) tuple on success, None on failure
    '''
    #tt = solr.current_milli_time()
    res = None
    try :
        l = requests.get(service_url, {'s': name}).json()
        if l:
            arr = l[0].values()[0]
            if arr:
                return {
                    'name' : arr[0],
                    'lat' : float(arr[2]),
                    'lon' : float(arr[1]),
                    'country': arr[3],
                    'state': arr[4],
                    'cityid': arr[5]
                }

    except Exception as e:
        print("Failed %s " % name)
    #print("%s \t = \t%s \t\t(%d ms)" % (name, res, (solr.current_milli_time() - tt)))
    return res


@lru_cache(maxsize=5000)
def get_gps(name):
    '''
    Gets GPS of a location
    :param name: location name
    :return: (latitude, longitude) tuple on success, None on failure
    '''
    tt = solr.current_milli_time()
    p = sp.Popen(['lucene-geo-gazetteer', '-s', '"%s"' % name],
                 stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = p.communicate()
    l = json.loads(out)
    res = None
    if l:
        arr = l[0].values()[0]
        if arr:
            res = (float(arr[2]), float(arr[1]))
    print("%s \t = \t%s \t\t(%d ms)" % (name, res, (solr.current_milli_time() - tt)))
    return res


def get_gps_updates(docs, idfield='id',
                    field='locations',
                    targetfield='location_geos'):


    usStatePat = re.compile("^[A-Z][A-Z]$")
    for doc in docs:
        if not(field in doc and idfield in doc):
            print("Skipped :: Invalid doc : %s" % doc)
            continue

        if field in doc:
            geos = set([])
            cities = set([])
            countries = set([])
            states = set([])

            for loc in doc.get(field):
                loc = get_gps_rest(loc)
                if loc:
                    locName = loc['name']
                    countries.add(loc['country'])
                    if loc['state'] and usStatePat.match(loc['state']):
                        states.add(loc['state'])
                    if loc['cityid'] and loc['cityid'] != '00':
                        # this is a city or small place
                        cities.add(locName)
                        # converting it to "lat,lon" string
                        geos.add("%s,%s" %(loc['lat'], loc['lon']))

            updates = {}
            if cities:
                updates['cities'] = {'set': list(cities)}
            if states:
                updates['states'] = {'set': list(states)}
            if countries:
                updates['countries'] = {'set': list(countries)}
            if geos:
                updates[targetfield] = {'set': list(geos)}
            if updates:
                updates[idfield] = doc[idfield]
                yield  updates


def get_docs(url, query, fields=None):
    '''
    gets a generator for yielding all the documents in solr index that matches to solr query
    :param url: the solr url
    :param query:  the query for selection
    :param fields: fields to be requested
    :return: generator of documents
    '''
    sc = solr.Solr(url)
    docs = sc.query_iterator(query, 0, rows=1000, fl=fields)
    return docs

if __name__ == '__main__':
    desturl = "http://localhost:8983/solr/collection2"
    srcurl = "http://localhost:8983/solr/collection3"
    qry = "locations:*"
    #qry = "id:\"http://palmdale.backpage.com/LostAndFound/\""
    docs = get_docs(srcurl, qry, fields=['id', 'locations'])
    #update_geo(docs)
    updates = get_gps_updates(docs)
    solr_client = solr.Solr(desturl)
    solr_client.post_iterator(updates, buffer_size=1000)