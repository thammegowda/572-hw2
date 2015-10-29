package edu.usc.cs.ir.cwork;

import edu.usc.cs.ir.cwork.solr.SolrIndexer;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
