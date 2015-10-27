from __future__ import print_function

import os
import requests as req

__author__ = 'tg'

def post_solrcell(solr_url, files):
    '''
    Posts the files to solr cell
    :param solr_url: url to solr core
    :param files: files to be posted
    :return: count
    '''
    count = 0
    url = solr_url
    if not url.endswith("/"):
        url += "/"
    url +="update/extract"

    for f in files:
        count += 0
        #using path as a docId
        u = url + "?uprefix=md_ss_&literal.id=" + f
        with open(f, 'rb') as payload:
            resp = req.post(u, files={'content':payload})
            print("%s :: %s" %(resp.status_code, f))

def find_files(path):
    '''
    Finds all the files inside the given directory
    :param path: path to parent directory
    :return: iterator of files  (recursive)
    '''
    if not os.path.isdir(path):
        raise Exception(path + " is not a directory ")
    for root, subdir, files in os.walk(path):
        for f in files:
            yield os.path.join(root, f)


if __name__ == '__main__':
    dump_dir = "/home/tg/work/coursework/cs572/hw1/sites/merged/dump/fc"
    solr_url = "http://localhost:8983/solr/weapons2"
    files = find_files(dump_dir)
    post_solrcell(solr_url, files)
    print("===Done===")
