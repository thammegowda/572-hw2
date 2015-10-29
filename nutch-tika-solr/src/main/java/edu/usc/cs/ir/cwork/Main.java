package edu.usc.cs.ir.cwork;

import edu.usc.cs.ir.cwork.nutch.RecordIterator;
import edu.usc.cs.ir.cwork.nutch.SegContentReader;
import edu.usc.cs.ir.cwork.solr.ContentBean;
import edu.usc.cs.ir.cwork.solr.SolrUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.protocol.Content;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tg on 10/25/15.
 */
public class Main {

    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public interface Command {

        String INDEX = "index";
        String STATS = "stats";

        String getName();

        void run() throws Exception;
    }

    @Argument(handler = SubCommandHandler.class, required = true,
    usage = "sub-command")
    @SubCommands({
            @SubCommand(name=Command.INDEX, impl = SolrIndexer.class),
            @SubCommand(name=Command.STATS, impl = Stats.class),
    })
    private Command cmd;

    public static class SolrIndexer implements Command{

        @Option(name = "-segs", aliases = {"--seg-paths"},
                usage = "Path to a text file containing segment paths. One path per line",
                required = true)
        private File segsFile;

        @Option(name = "-url", aliases = {"--solr-url"},
                usage = "Solr url", required = true)
        private URL solrUrl;

        private Main context;


        public String getName() {
            return INDEX;
        }

        public void run() throws IOException, InterruptedException, SolrServerException {

            SolrServer solr = new HttpSolrServer(solrUrl.toString());
            FileInputStream stream = new FileInputStream(segsFile);
            List<String> paths = IOUtils.readLines(stream);
            IOUtils.closeQuietly(stream);
            LOG.info("Found {} lines in {}", paths.size(), segsFile.getAbsolutePath());
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
                ContentBean bean = SolrUtil.createBean(content, true);
                beans.add(bean);
                count++;
                if (beans.size() >= batchSize) {
                    solr.addBeans(beans);
                    beans.clear();
                }

                if (System.currentTimeMillis() - st > delay) {
                    LOG.info("Num Docs : {}", count);
                    st = System.currentTimeMillis();
                }
            }

            //left out
            if (!beans.isEmpty()) {
                solr.addBeans(beans);
            }

            // commit
            LOG.info("Committing before exit. Num Docs = {}", count);
            UpdateResponse response = solr.commit();
            LOG.info("Commit response : {}", response);
        }
    }

    public static class Stats implements Command {

        public String getName() {
            return STATS;
        }

        public void run() {
            throw new RuntimeException("Not implemented");
        }
    }

    public static void main(String[] args) throws Exception {
        args = "index -segs data/paths-all.txt -url http://localhost:8983/solr".split(" ");
        Main main = new Main();
        CmdLineParser parser = new CmdLineParser(main);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.out);
            System.err.println(e.getMessage());
            return;
        }
        LOG.debug("Command={}", main.cmd.getName());
        main.cmd.run();
    }

}
