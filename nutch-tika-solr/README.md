Nutch Tika Solr
================

This project contains course work of 'Information Retrieval and web Search Engines' (CSCI 572) 
course of University of Southern California.
The main theme of this project is building inverted index using `Apache Lucene/Solr`. The data is crawled from web
using `Apache Nutch` and it is read from segments using `Apache Hadoop-HDFS` API.
Additional enrichment to documents is made by parsing documents with `Apache Tika`.

# Requirements 
+ JDK 1.8  
+ Newer version of Maven (used 3.3)
+ Internet connection to download maven dependencies

# Additional Setup 
During the course of this project, we enhanced `Apache Tika` by adding a `NamedEntityParser` and supplied an
implementation of Named Entity Recogniser based on _StanfordCoreNLP_. These changes are not reached to mainstream
so the following setup is necessary prior to building this project.

+ Build Latest Tika with Named Entity Recogniser
  
  + `git clone git@github.com:thammegowda/tika.git`
  + `mvn clean install` (# may be `mvn clean install -DskipTests`)
+ Build Tika CoreNlp addon NLP
  + `git clone git@github.com:thammegowda/tika-ner-corenlp.git`
  + `mvn install`

# How to build.

After completing the _Additional Setup_ process, the build is as simple as
+ `mvn exec:java -Dexec.args=` to run
+ `mvn clean package assembly:single` to package


# How to run

This project offers sub commands.

  ```
  Usage : Main <CMD>
  The following command(CMD)s are available
          index : Index nutch segments to solr
  ```

  + **index** Command

  Usage :
  ```
  java -jar target/nutch-tika-solr-1.0-SNAPSHOT-jar-with-dependencies.jar index
  Option "-segs (--seg-paths)" is required
   -batch (--batch-size) N  : Number of documents to buffer and post to solr
                              (default: 1000)
   -segs (--seg-paths) FILE : Path to a text file containing segment paths. One
                              path per line
   -url (--solr-url) URL    : Solr url
  ```

  Example :
  ```
  java -jar target/nutch-tika-solr-1.0-SNAPSHOT-jar-with-dependencies.jar index -batch 300 -segs data/paths-all.txt -url http://localhost:8983/solr
  ```

//FIXME: update this


# Developers / Team
+ Thamme Gowda N.
+ Rakshith
+ Rahul
+ Nii Mante
