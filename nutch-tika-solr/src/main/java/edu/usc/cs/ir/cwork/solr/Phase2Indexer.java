package edu.usc.cs.ir.cwork.solr;

import edu.usc.cs.ir.cwork.solr.schema.FieldMapper;
import edu.usc.cs.ir.cwork.tika.Parser;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.*;

/**
 * This class accepts CLI args containing paths to Nutch segments and solr Url,
 * then runs the index operation optionally reparsing the metadata.
 */
public class Phase2Indexer {

    public static final String MD_SUFFIX = "_md";
    private static Logger LOG = LoggerFactory.getLogger(SolrIndexer.class);

    @Option(name = "-src", aliases = {"--src-solr"},
            usage = "Source Solr url", required = true)
    private URL srcSolr;

    @Option(name = "-dest", aliases = {"--dest-solr"},
            usage = "Destination Solr url", required = true)
    private URL destSolr;

    @Option(name = "-batch", aliases = {"--batch-size"},
            usage = "Number of documents to buffer and post to solr",
            required = false)
    private int batchSize = 1000;

    @Option(name = "-start", aliases = {"--start"},
            usage = "Import start",
            required = false)
    private int start = 0;


    @Option(name = "-q", aliases = {"--query"},
            usage = "Import Query",
            required = false)
    private String queryStr = "*:*";


    public FieldMapper mapper = FieldMapper.create();


    public Map<String, String> map = new HashMap<>();
    {
        map.put("NER_PERSON", "persons");
        map.put("NER_LOCATION", "locations");
        map.put("NER_ORGANIZATION", "organizations");
        map.put("NER_PHONE_NUMBER", "phonenumbers");
        map.put("NER_WEAPON_NAME", "weaponnames");
        map.put("NER_WEAPON_TYPE", "weapontypes");
    }

    private String[] copyFields = {"id", "title", "content",
            "contentLength", "boost", "lastModified", "digest", "host"};

    private Set<String> textFields = new HashSet<>(
            Arrays.asList("id", "title", "content", "lastModified"));

    /**
     * runs the solr index command
     * @throws IOException
     * @throws InterruptedException
     * @throws SolrServerException
     */
    public void run() throws Exception {

        SolrServer solrServer = new HttpSolrServer(srcSolr.toString());
        SolrDocIterator docs = new SolrDocIterator(solrServer, queryStr,
                start, batchSize, copyFields);
        parseAndUpdate(docs);

    }

    private void parseAndUpdate(SolrDocIterator docs)
            throws IOException, SolrServerException {

        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;

        SolrServer destSolr = new HttpSolrServer(this.destSolr.toString());
        List<SolrInputDocument> buffer = new ArrayList<>(batchSize);
        while (docs.hasNext()) {
            SolrDocument doc = docs.next();
            SolrInputDocument delta = new SolrInputDocument();
            StringBuilder sb = new StringBuilder();

            for (String field : doc.getFieldNames()) {
                delta.setField(field, doc.get(field));
                if (textFields.contains(field)) {
                    sb.append(doc.get(field)).append("\n");
                }
            }

            Parser parser = Parser.getPhase2Parser();
            String content = sb.toString();
            org.apache.tika.metadata.Metadata md = parser.parseContent(content);

            for (String name : md.names()) {
                Serializable value = md.isMultiValued(name) ? md.getValues(name) : md.get(name);
                if (map.containsKey(name)) {
                    delta.setField(map.get(name), value);
                } else {
                    String newName = mapper.mapField(name, value);
                    if (newName != null) {
                        newName += MD_SUFFIX;
                        delta.setField(newName, value);
                    }
                }
            }

            Set<Date> dates = Parser.parseDates(content);
            if (dates != null && !dates.isEmpty()) {
                delta.addField("dates", dates);
            }
            count++;
            buffer.add(delta);
            if (buffer.size() >= batchSize) {
                destSolr.add(buffer);
                buffer.clear();
            }

            if (System.currentTimeMillis() - st > delay) {
                LOG.info("Num Docs : {},  Imported {} of {}", count,
                        docs.getNextStart(), docs.getNumFound());
                st = System.currentTimeMillis();
            }
        }
        //left out
        if (!buffer.isEmpty()) {
            destSolr.addBeans(buffer);
        }
        LOG.info("Committing before exit. Num Docs = {}", count);
        UpdateResponse response = destSolr.commit();
        LOG.info("Commit response : {}", response);
    }

    public static void main(String[] args) throws Exception {

       //args = "-batch 10 -src http://localhost:8983/solr/weapons1 -dest http://localhost:8983/solr/collection1".split(" ");
        Phase2Indexer indexer = new Phase2Indexer();
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
