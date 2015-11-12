__author__="Nii Mante"
__license__="Apache"

import argparse
import json
import sys

def create_parser():
    parser = argparse.ArgumentParser(description="This program converts the results from page rank into an easily readable/consumable JSON file for d3")
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-e', '--edge_file', type=argparse.FileType('r'), help="""
        A file containing nodes and connected nodes (adjacency list).
        <NODE> <NODE_2> 
        <NODE_2>
        """)
    group.add_argument('-p', '--pr_file', type=argparse.FileType('r'), help="""
        A file containing tab nodes and page ranks.
        <NODE> <PAGE_RANK>
        """)
    parser.add_argument('-o', '--out_file',  type=str, default="d3_out.json", help="The file to output your d3 json to")

    return parser

def generate_bubble_json(args):
    pr_dict = {}
    pr_dict["d3"] = []
    pr_file = args.pr_file

    for line in pr_file:
        node, pr = line.split()
        print >> sys.stderr, node
        print >> sys.stderr, pr
        pr_dict["d3"].append({"url" : node, "pr" : pr})
        
    print >> sys.stderr, "Printing d3 JSON to %s" % args.out_file
    with open(args.out_file, 'w') as pr_file:
        json.dump(pr_dict, pr_file, indent=4) 

def generate_graph_json(args):
    pass

def main():
    parser = create_parser()
    args = parser.parse_args()

    if args.pr_file != None:
        generate_bubble_json(args)
    
    if args.edge_file != None:
        generate_graph_json(args)


if __name__=="__main__":
    main()

