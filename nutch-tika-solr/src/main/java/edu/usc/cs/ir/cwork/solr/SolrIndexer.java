package edu.usc.cs.ir.cwork.solr;

import edu.usc.cs.ir.cwork.Main;
import edu.usc.cs.ir.cwork.nutch.RecordIterator;
import edu.usc.cs.ir.cwork.nutch.SegContentReader;
import edu.usc.cs.ir.cwork.tika.Parser;
import edu.usc.cs.ir.solr.dynschema.FieldMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.tika.parser.ner.NERecogniser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tika.parser.ner.NERecogniser.*;

/**
 * Created by tg on 10/29/15.
 */
public class SolrIndexer implements Main.Command {


    public static final String MD_SUFFIX = "_md";
    private static Logger LOG = LoggerFactory.getLogger(SolrIndexer.class);
    private static Set<String> TEXT_TYPES = new HashSet<>(Arrays.asList("html", "xhtml", "xml", "plain", "xhtml+xml"));

    @Option(name = "-segs", aliases = {"--seg-paths"},
            usage = "Path to a text file containing segment paths. One path per line",
            required = true)
    private File segsFile;

    @Option(name = "-url", aliases = {"--solr-url"},
            usage = "Solr url", required = true)
    private URL solrUrl;

    private Main context;

    public FieldMapper mapper = FieldMapper.create();
    private Parser parser;

    public String getName() {
        return INDEX;
    }


    /**
     * Creates Solrj Bean from nutch content
     *
     * @param content the nutch content
     * @param reparse should  the tika reparse metadata
     * @return Solrj Bean
     */
    public ContentBean createBean(Content content,
                                  boolean reparse) {

        ContentBean bean = new ContentBean();
        bean.setContentType(content.getContentType());
        bean.setUrl(content.getUrl());

        Map<String, Object> mdFields = new HashMap<>();

        Metadata metadata = content.getMetadata();
        if (reparse) {
            try {
                parser = Parser.INSTANCE;
                Pair<String, org.apache.tika.metadata.Metadata> pair =  parser.parse(content);
                bean.setContent(pair.getFirst());
                org.apache.tika.metadata.Metadata tikaMd = pair.getSecond();
                metadata = new Metadata();
                for (String name : tikaMd.names()) {
                    String[] values = tikaMd.isMultiValued(name) ?
                            tikaMd.getValues(name) :
                            new String[]{tikaMd.get(name)};
                    for (String value : values) {
                        metadata.add(name, value);
                    }
                }
            } catch ( Exception e){
                LOG.info("Parse Failed for {} : {}. Msg:{}", content.getUrl(),
                        content.getContentType(), e.getMessage());
            }
        } else if ("text".equals(bean.getMainType())
                || TEXT_TYPES.contains(bean.getSubType().toLowerCase())){
            bean.setContent(new String(content.getContent()));
        }

        for (String name : metadata.names()) {
            boolean special = false;
            if (name.startsWith("NER_"))  {
                special = true; //could be special
                String nameType = name.substring("NER_".length());
                if (DATE.equals(nameType)) {
                    Set<Date> dates = parser.parseDates(metadata.getValues(name));
                    bean.setDates(dates);
                } else if (PERSON.equals(nameType)){
                    bean.setPersons(asSet(metadata.getValues(name)));
                } else if (ORGANIZATION.equals(nameType)) {
                    bean.setOrganizations(asSet(metadata.getValues(name)));
                } else if (LOCATION.equals(nameType)) {
                    bean.setLocations(asSet(metadata.getValues(name)));
                } else {
                    //no special casing this field!!
                    special = false;
                }
            }

            if (!special) {
                mdFields.put(name, metadata.isMultiValued(name)
                        ? metadata.getValues(name) : metadata.get(name));
            }
        }

        Map<String, Object> mappedMdFields = mapper.mapFields(mdFields, true);
        Map<String, Object> suffixedFields = new HashMap<>();
        mappedMdFields.forEach((k, v) -> {
            if (!k.endsWith(MD_SUFFIX)) {
                k += MD_SUFFIX;
            }
            suffixedFields.put(k, v);
        });

        bean.setMetadata(suffixedFields);
        return bean;
    }

    /**
     * Converts an array into set
     * @param items array of items
     * @param <T>
     * @return set created from array
     */
    public static <T>  Set<T> asSet(T...items) {
        HashSet<T> set = new HashSet<>();
        Collections.addAll(set, items);
        return set;
    }

    /**
     * runs the solr index command
     * @throws IOException
     * @throws InterruptedException
     * @throws SolrServerException
     */
    public void run() throws IOException, InterruptedException, SolrServerException {

        SolrServer solr = new HttpSolrServer(solrUrl.toString());
        FileInputStream stream = new FileInputStream(segsFile);
        List<String> paths = IOUtils.readLines(stream);
        IOUtils.closeQuietly(stream);
        Main.LOG.info("Found {} lines in {}", paths.size(), segsFile.getAbsolutePath());
        SegContentReader reader = new SegContentReader(paths);
        RecordIterator recs = reader.read();
        index(recs, solr);
        System.out.println(recs.getCount());
    }

    private void index(RecordIterator recs, SolrServer solr) throws IOException, SolrServerException {
        int batchSize = 1000;
        List<ContentBean> beans = new ArrayList<>(batchSize);
        long st = System.currentTimeMillis();
        long count = 0;
        long delay = 2 * 1000;

        while (recs.hasNext()) {
            Pair<String, Content> rec = recs.next();
            Content content = rec.getValue();
            ContentBean bean = createBean(content, true);
            beans.add(bean);
            count++;
            if (beans.size() >= batchSize) {
                try {
                    solr.addBeans(beans);
                    beans.clear();
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
        if (!beans.isEmpty()) {
            solr.addBeans(beans);
        }

        // commit
        Main.LOG.info("Committing before exit. Num Docs = {}", count);
        UpdateResponse response = solr.commit();
        Main.LOG.info("Commit response : {}", response);
    }
}
