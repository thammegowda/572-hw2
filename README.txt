Search Engine Assignment of USC CSCI 572
=========================================

Project Structure
-----------------

+ dump-poster      : This directory contains python program to post documents to Solr Cell
+ nutch-tika-solr  : This is the main project which includes nutch content reader, tika parser,
                      solr indexer, graph generator, page rank computer and solr document updater
+ query Runner     : This directory contains python script to execute challenge questions
+ d3               : This directory contains python script for converting output of page rank
                    computer to json file and D3 visualization files
+ weapons-ner-dataset : This directory contains dataset we used to train NER model for weapons,
                        and also to build regex 

# The tutorial/usage instructions file
--------------------------------------
nutch-tika-solr/README.md        : this file explains how to setup the environment and build the package
nutch-tika-solr/step-by-step.txt : this file explains how to run the code
query-runner/README.md           : this file explains how to run challenge queries of assignment
d3/README.md                     : this file explains how to run d3 visualization


Solr config files:
------------------
nutch-tika-solr/conf/solrconfig.xml : the solr config file
nutch-tika-solr/conf/schema.xml     : the schema file
nutch-tika-solr/conf/stopwords.txt  : The stopwords
