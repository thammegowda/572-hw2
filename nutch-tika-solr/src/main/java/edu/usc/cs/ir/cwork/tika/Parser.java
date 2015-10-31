package edu.usc.cs.ir.cwork.tika;

import edu.usc.cs.ir.tika.ner.corenlp.CoreNLPNERecogniser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.protocol.Content;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ner.NamedEntityParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;

/**
 * Created by tg on 10/25/15.
 */
public enum Parser {
    INSTANCE;

    public final Logger LOG = LoggerFactory.getLogger(Parser.class);

    private Tika tika;

    Parser() {
        URL confFile = getClass().getClassLoader().getResource("tika-config.xml");
        if (confFile != null) {
            LOG.info("Found tika conf at  {}", confFile);
            try {
                System.setProperty(NamedEntityParser.SYS_PROP_NER_IMPL, CoreNLPNERecogniser.class.getName());
                TikaConfig config = new TikaConfig(confFile);
                tika = new Tika(config);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("No Tika conf found");
        }
    }

    /**
     * Parses the content
     * @param content  the nutch content to be parsed
     * @return the text content
     */
    public String parseContent(Content content){
        Pair<String, Metadata> pair = parse(content);
        return pair != null ? pair.getKey() : null;
    }

    /**
     * Parses Nutch content to read text content and metadata
     * @param content nutch content
     * @return pair of text and metadata
     */
    public Pair<String, Metadata> parse(Content content){
        ByteArrayInputStream stream = new ByteArrayInputStream(content.getContent());
        try {
            return parse(stream);
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    /**
     * Parses the stream to read text content and metadata
     * @param stream the stream
     * @return pair of text content and metadata
     */
    private Pair<String, Metadata> parse(ByteArrayInputStream stream) {
        Metadata metadata = new Metadata();
        try {
            String text = tika.parseToString(stream, metadata);
            return new Pair<>(text, metadata);
        } catch (IOException | TikaException e) {
            LOG.warn(e.getMessage(), e);
        }
        //something bad happened
        return null;
    }

    public static void main(String[] args) {
        Parser parser = INSTANCE;

        ByteArrayInputStream stream = new ByteArrayInputStream("Los Angeles".getBytes());
        Pair<String, Metadata> pair = parser.parse(stream);
        System.out.println(pair.getValue());
    }
}
