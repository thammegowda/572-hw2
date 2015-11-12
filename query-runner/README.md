#Query Runner

Nii Mante

##Overview

This program runs a list of lucene queries by posting the queries to Solr.  
It returns the results in a readable format.

##Requirements

	pip install requests
	pip install colorama

##Example Usage

	python query-runner.py -u http://localhost:8983/solr -c collection2 -f queries.txt


##Usage

	usage: query-runner.py [-h] [-u SOLR_HOST] [-c COLLECTION]
	                       [-f QUERY_FILE | -j JSON_QUERY_FILE]
	
	Given a filename containing lucene queries, this program executes the queries
	and gives the results in a readable file
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -u SOLR_HOST, --solr_host SOLR_HOST
	                        The solr host url. (e.g., http://localhost:8983/solr
	  -c COLLECTION, --collection COLLECTION
	                        The collection/core name. Default is 'collection1'
	  -f QUERY_FILE, --query_file QUERY_FILE
	                        A file containing lucene queries line by line. (e.g.,
	                        query?q=rifles+in+texas)
	  -j JSON_QUERY_FILE, --json_query_file JSON_QUERY_FILE
	                        A json file containing queries to be posted in a JSON
	                        solr query format
