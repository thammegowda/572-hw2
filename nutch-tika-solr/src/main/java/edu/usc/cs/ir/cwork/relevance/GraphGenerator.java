package edu.usc.cs.ir.cwork.relevance;

import edu.usc.cs.ir.cwork.solr.SolrDocIterator;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;

/**
 * This class offers functionality for graph creation and also provides command line interface.
 * @author Thamme Gowda N
 * @since Nov. 6th, 2015
 */
public class GraphGenerator {

    /**
     * Delay between progress updates
     */
    public static final int DELAY = 2000;
    public static final Logger LOG = LoggerFactory.getLogger(GraphGenerator.class);

    public enum EdgeType {
        //NOTE: these are the fields in solr
        locations,
        persons,
        dates,
        organizations;
    }

    @Option(name = "-solr", usage = "Solr URL to query docs", required = true)
    private URL solrUrl;

    @Option(name = "-out", usage = "Output File for writing the edges of graph.", required = true)
    private File outputFile;

    @Option(name = "-edge", usage = "Edge type. This should be a field in solr docs.", required = true)
    private EdgeType edgeType;

    /**
     * Generates sequence of edges by querying and joining docs in solr
     * @return number of edges
     * @throws IOException
     */
    private long generate() throws IOException {

        long edgeCount = 0;
        long vertexCount = 0;
        long st = System.currentTimeMillis();
        SolrServer solr = new HttpSolrServer(solrUrl.toString());
        String field = edgeType.name();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))){
            String idField = "id";
            String locationJoin = "{!join from="+ field +" to=" + field + "}";

            // for each doc that has locations
            SolrDocIterator iterator = new SolrDocIterator(solr, field + ":*", idField);
            while (iterator.hasNext()){
                SolrDocument doc = iterator.next();
                String id1 = (String) doc.getFieldValue(idField);

                // join with other docs that has same edge field
                String joinQuery = locationJoin + idField +  ":\"" + id1 + "\"";

                //get connected docs
                SolrDocIterator connectedDocs = new SolrDocIterator(solr, joinQuery, idField);
                while (connectedDocs.hasNext()) {
                    SolrDocument doc2 = connectedDocs.next();
                    String id2 = (String) doc2.get(idField);
                    if (id1.equals(id2)) {
                        continue; //skip
                    }
                    //draw an edge
                    writer.write(id1 + "\t" + id2 + "\n");
                    edgeCount++;
                }
                vertexCount++;
                if (System.currentTimeMillis() - st > DELAY) {
                    st = System.currentTimeMillis();
                    LOG.info("Vertices : {}, Edges {}", vertexCount, edgeCount);
                }
            }
        }
        return edgeCount;
    }

    public static void main(String[] args) throws IOException {
       //args = "-solr http://localhost:8983/solr/ -out locations.txt -edge locations".split(" ");

        GraphGenerator graphGen = new GraphGenerator();
        CmdLineParser parser = new CmdLineParser(graphGen);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getLocalizedMessage());
            parser.printUsage(System.err);
            return;
        }
        graphGen.generate();
        long edgeCount = graphGen.generate();
        System.out.println("Total Edges : " + edgeCount);

    }
}
