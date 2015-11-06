package edu.usc.cs.ir.cwork.solr;

/**
 * Created by nmante on 11/4/15.
 */

import edu.usc.cs.ir.cwork.Main;
import edu.usc.cs.ir.cwork.graph.Graph;
import edu.usc.cs.ir.cwork.graph.Vertex;
import edu.usc.cs.ir.cwork.nutch.RecordIterator;
import edu.usc.cs.ir.cwork.nutch.SegContentReader;
import edu.usc.cs.ir.cwork.solr.schema.FieldMapper;
import edu.usc.cs.ir.cwork.tika.Parser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.apache.tika.parser.ner.NERecogniser.*;

/**
 * This class accepts CLI args containing paths to Nutch segments and solr Url,
 * then runs the index operation optionally reparsing the metadata.
 */
public class SolrPageRankUpdater {

    public static final String MD_SUFFIX = "_md";
    private static Logger LOG = LoggerFactory.getLogger(SolrPageRankUpdater.class);
    private static Set<String> TEXT_TYPES = new HashSet<>(Arrays.asList("html", "xhtml", "xml", "plain", "xhtml+xml"));


    private String solrUrl;

    private boolean reparse = true;

    private int batchSize = 1000;

    public FieldMapper mapper = FieldMapper.create();
    private Parser parser;
    private Graph graph;

    public SolrPageRankUpdater(Graph graph, String url){
        this.graph = graph;
        this.solrUrl = url;
    }

    /**
     * Given a graph, post page rank scores of documents to Solr
     *
     * @throws IOException
     * @throws SolrServerException
     */

    public void run() throws IOException, SolrServerException{
        // create the SolrJ Server
        HttpSolrServer solr = new HttpSolrServer(solrUrl);


        Set<Vertex> vertices = graph.getVertices();
        ArrayList<SolrInputDocument> solrDocs = new ArrayList<>(batchSize);
        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;

        for (Vertex v : vertices){
            // create the document
            SolrInputDocument sDoc = new SolrInputDocument();

            // Add the id field, and page rank field to the document
            sDoc.addField("id", v.getId());
            Map<String,Double> fieldModifier = new HashMap<>();
            fieldModifier.put("set", v.getScore());
            sDoc.addField(graph.getType(), fieldModifier);  // add the map as the field value
            System.out.print(sDoc);
            solrDocs.add(sDoc);

            count++;
            if (solrDocs.size() >= batchSize) {
                try {
                    solr.add(solrDocs);
                    solrDocs.clear();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            if (System.currentTimeMillis() - st > delay) {
                Main.LOG.info("Num Docs : {}", count);
                st = System.currentTimeMillis();
            }
        }

        //left out
        if (!solrDocs.isEmpty()) {
            solr.add(solrDocs);
        }

        // commit
        Main.LOG.info("Committing before exit. Num Docs = {}", count);
        UpdateResponse response = solr.commit();
        solr.shutdown();
        Main.LOG.info("Commit response : {}", response);
    }

    public static void main(String[] args) throws InterruptedException,
            SolrServerException, IOException {
        SolrIndexer indexer = new SolrIndexer();
        CmdLineParser cmdLineParser = new CmdLineParser(indexer);
        try {
            cmdLineParser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            cmdLineParser.printUsage(System.out);
            return;
        }
        indexer.run();
        System.out.println("Done");
    }
}
