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
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tg on 10/29/15.
 */
public class SolrIndexer implements Main.Command {

    @Option(name = "-segs", aliases = {"--seg-paths"},
            usage = "Path to a text file containing segment paths. One path per line",
            required = true)
    private File segsFile;

    @Option(name = "-url", aliases = {"--solr-url"},
            usage = "Solr url", required = true)
    private URL solrUrl;

    private Main context;

    public FieldMapper mapper = FieldMapper.create();


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
            Pair<String, org.apache.tika.metadata.Metadata> pair = Parser.INSTANCE.parse(content);
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
        }
        String[] names = metadata.names();
        for (String name : names) {

            Serializable val = metadata.isMultiValued(name) ?
                    metadata.getValues(name) :
                    metadata.get(name);
            mdFields.put(name, val);
        }

        bean.setMetadata(mapper.mapFields(mdFields, true));
        return bean;
    }

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
                solr.addBeans(beans);
                beans.clear();
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
