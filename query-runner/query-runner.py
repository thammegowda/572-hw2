__author__ = "Nii Mante"
__licence__ = "Apache"
__email__ = "nmante@usc.edu"
__status__ = "Development"

import argparse
import requests
import sys
import json

def create_parser():
    parser = argparse.ArgumentParser(description="Given a filename containing lucene queries, this program executes the queries and gives the results in a readable file")
    parser.add_argument('-u', '--solr_host', help="The solr host url. (e.g., http://localhost:8983/solr", default="http://localhost:8983/solr")
    parser.add_argument('-c', '--collection', help="The collection/core name. Default is 'collection1'", default="collection1", type=str)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-f', '--query_file', type=argparse.FileType('r'), help="A file containing lucene queries line by line. (e.g., query?q=rifles+in+texas)")
    group.add_argument('-j', '--json_query_file', type=argparse.FileType('r'), help="A json file containing queries to be posted in a JSON solr query format")
    return parser

def run_queries(args):

    # The url for your solr host (e.g. http://localhost:8983/solr
    solr_host_url = args.solr_host

    # The collection name (e.g. 'collection3')
    solr_core = args.collection
    
    # A file object. The file contains lines of queries
    queries = args.query_file if args.query_file != None else args.json_query_file
    queries = open('queries.txt','r') if queries == None else queries
    
    # Iterate over the query file 

    for query_line in queries:
        response = requests.get(solr_host_url + "/" + solr_core + "/" + query_line)
        print >> sys.stderr, "Response for query: %s" % query_line 
        print json.dumps(response.json(), indent=4, sort_keys=True)


def main():
    parser = create_parser()
    print "hello"
    args = parser.parse_args()
    run_queries(args)

if __name__ == "__main__":
    main()
