package edu.usc.cs.ir.cwork.nutch;

import edu.usc.cs.ir.cwork.Context;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by tg on 10/25/15.
 */
public class RecordIterator implements Iterator<Pair<String, Content>> {

    public static final Logger LOG = LoggerFactory.getLogger(RecordIterator.class);

    private Iterator<Path> paths;
    private Configuration conf;
    private FileSystem fs;
    private long count = 0;
    private long errorCount = 0;

    private SequenceFile.Reader reader;
    private Pair<String, Content> next;
    private Text key = new Text(); // reused
    private Content value; // not re used, so created when needed

    public RecordIterator(List<Path> paths) {
        this.paths = paths.iterator();
        this.conf = Context.getInstance().getConf();
        this.fs = Context.getInstance().getFs();
        this.next = this.getNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Pair<String, Content> next() {
        Pair<String, Content> tmp = next;
        next = this.getNext();
        return tmp;
    }

    private Pair<String, Content> getNext() {
        if (reader != null ) {
            value = new Content();
            try {
                if (reader.next(key, value)) {
                    count++;
                    return new Pair<>(key.toString(), value);
                }
            } catch (IOException e) {
                errorCount++;
                LOG.warn(e.getMessage(), e);
            }
            //exception or reached the end of loop
            IOUtils.closeQuietly(reader);
            reader = null;
        }

        while (paths.hasNext()) {
            try {
                //open a new reader
                reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(paths.next()));
                ///read from new reader
                return getNext();
            } catch (IOException e) {
                errorCount++;
                LOG.warn(e.getMessage(), e);
            }
        }

        //end of content
        return null;
    }

    public long getCount() {
        return count;
    }

    public long getErrorCount() {
        return errorCount;
    }
}
