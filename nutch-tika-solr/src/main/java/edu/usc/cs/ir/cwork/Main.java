package edu.usc.cs.ir.cwork;

import edu.usc.cs.ir.cwork.relevance.GraphGenerator;
import edu.usc.cs.ir.cwork.relevance.SparkPageRanker;
import edu.usc.cs.ir.cwork.solr.SolrIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * This class offers CLI interface for the project
 */
public class Main {

    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    /**
     * Enumerate all the known sub-commands
     */
    private enum Cmd {
        index("Index nutch segments to solr"),
        graph("Builds a graph of documents, and writes the edges set to file "),
        pagerank("Computes page rank for nodes in graph");

        private final String description;

        Cmd(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    public static Cmd getCommand(String cmdName) {
        try {
            return Cmd.valueOf(cmdName);
        } catch (Exception e) {
            System.err.println("Unknown command " + cmdName);
            printUsage(System.err);
            System.exit(2);
            throw new IllegalArgumentException("Unknown command " + cmdName);
        }
    }

    public static void printUsage(PrintStream out){
        out.println("Usage : Main <CMD>");
        out.println("The following command(CMD)s are available");
        for (Cmd cmd : Cmd.values()) {
            out.printf("%12s :  %s", cmd.name(), cmd.getDescription());
            out.println();
        }
        out.println();
        out.flush();
    }

    public static void main(String[] args) throws Exception {
        //args = "index -segs data/paths.txt -url http://localhost:8983/solr".split(" ");
        if (args.length == 0) {
            printUsage(System.out);
            System.exit(1);
        }
        Cmd cmd = getCommand(args[0]);  // the first argument has to be positional parma
        String subCmdArgs[] = new String[args.length-1];
        System.arraycopy(args, 1, subCmdArgs, 0, args.length - 1);
        switch (cmd) {
            case index:
                SolrIndexer.main(subCmdArgs);
                break;
            case graph:
                GraphGenerator.main(subCmdArgs);
                break;
            case pagerank:
                SparkPageRanker.main(subCmdArgs);
                break;
            default:
                throw new IllegalStateException(cmd.name() + " : not implemented");
        }
    }

}
