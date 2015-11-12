#D3 Converter Program

Given a page rank file containing nodes, and there page ranks separated by space

	<URL> <PAGE_RANK>

This program outputs a JSON file for D3 visualization.

##Example Usage
	
	python converter.py -p pageranks-locations.txt -o d3_out.json

##Usage

	usage: converter.py [-h] [-e EDGE_FILE | -p PR_FILE] [-o OUT_FILE]

	This program converts the results from page rank into an easily
	readable/consumable JSON file for d3

	optional arguments:
	  -h, --help            show this help message and exit
	  -e EDGE_FILE, --edge_file EDGE_FILE
				A file containing nodes and connected nodes (adjacency
				list). <NODE> <NODE_2> <NODE_2>
	  -p PR_FILE, --pr_file PR_FILE
				A file containing tab nodes and page ranks. <NODE>
				<PAGE_RANK>
	  -o OUT_FILE, --out_file OUT_FILE
				The file to output your d3 json to
