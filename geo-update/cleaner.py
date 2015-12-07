import requests as req
import notes.util.solr as solr
import subprocess as sp
import json
from repoze.lru import lru_cache
import requests
import re

def getHosts(solrurl):
    url = solrurl + "/query?q=*:*&rows=0&facet=true&facet.field=host&facet.limit=10000"
    hosts = req.get(url).json()['facet_counts']['facet_fields']['host']
    return dict(zip(hosts[::2], hosts[1::2]))


def dumpall(sClient, filename):
    #step 1: getall and store it
    itr = sClient.query_iterator("*:*", rows=1000)
    c = 0
    with open(filename, 'w') as w:

        for d in itr:
            w.write(json.dumps(d))
            w.write("\n")
            c += 1
    return c

def readDump(filename):
    '''
    Reads json line dump
    :param filename: name of file
    :return:  docs strem
    '''
    with open(filename, 'r') as r:
        for l in r:
            yield json.loads(l)


def cleanStrings(strings):
    return map(lambda s :s[0].upper() + s[1:],
               set(map(lambda s: s.lower(),
                       filter(lambda s: len(s.strip()) > 0, strings))))


def findUpdates(docStream):
    for d in docStream:
        update = {}
        # 2 Clean weapon names
        if 'weaponnames' in d:
            update['weaponnames'] = {'set' : cleanStrings(d['weaponnames'])}

        # 3 Clean weapon types
        if 'weapontypes' in d:
            update['weapontypes'] = {'set' : cleanStrings(d['weapontypes'])}
        if len(update) > 0:
            update['id'] = d['id']
            yield update

def cleanWeapons(solrurl):
    sc = solr.Solr(solrurl)

    #step 1: getall and store it
    tempfile = "alldocs.jsonl"
    #num = dumpall(sc, tempfile)

    updates = findUpdates(readDump(tempfile))
    sc.post_iterator(updates, buffer_size=1000)



if __name__ == '__main__':
    desturl = "http://localhost:8983/solr/collection2"
    docs = readDump("alldocs.jsonl")
    solr_client = solr.Solr(desturl)


