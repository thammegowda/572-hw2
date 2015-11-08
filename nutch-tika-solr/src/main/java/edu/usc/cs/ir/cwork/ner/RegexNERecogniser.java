package edu.usc.cs.ir.cwork.ner;

import org.apache.commons.io.IOUtils;
import org.apache.tika.parser.ner.NERecogniser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Entity name recogniser using regex
 * An implementation of {@link NERecogniser}
 *
 * @author Thamme Gowda N
 * @since Nov. 7, 2015
 */
public class RegexNERecogniser implements NERecogniser {

    private static Logger LOG = LoggerFactory.getLogger(RegexNERecogniser.class);
    private static final String NER_REGEX_FILE = "ner-regex.txt";

    public Set<String> entityTypes = new HashSet<>();
    public Map<String, Pattern> patterns;
    private boolean available = false;

    private static RegexNERecogniser INSTANCE;

    private RegexNERecogniser(){
        this(RegexNERecogniser.class.getClassLoader().getResourceAsStream(NER_REGEX_FILE));
    }

    public RegexNERecogniser(InputStream stream){
        try {
            patterns = new HashMap<>();
            List<String> lines = IOUtils.readLines(stream);
            IOUtils.closeQuietly(stream);
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")){ //empty or comment
                    //skip
                    continue;
                }

                int delim = line.indexOf('=');
                if (delim < 0) { //delim not found
                    //skip
                    LOG.error("Skip : Invalid config : " + line);
                    continue;
                }
                String type = line.substring(0, delim).trim();
                String patternStr = line.substring(delim+1, line.length()).trim();
                patterns.put(type, Pattern.compile(patternStr));
                entityTypes.add(type);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        available = !entityTypes.isEmpty();
    }

    public synchronized static RegexNERecogniser getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RegexNERecogniser();
        }
        return INSTANCE;
    }

    @Override
    public boolean isAvailable() {
        return available;
    }

    @Override
    public Set<String> getEntityTypes() {
        return entityTypes;
    }

    /**
     * finds matching sub groups in text
     * @param text text containing interesting sub strings
     * @param pattern pattern to find sub strings
     * @return set of sub strings if any found, or null if none found
     */
    public Set<String> findMatches(String text, Pattern pattern){
        Set<String> results = null;
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            results = new HashSet<>();
            results.add(matcher.group(0));
            while (matcher.find()) {
                results.add(matcher.group(0));
            }
        }
        return results;
    }

    @Override
    public Map<String, Set<String>> recognise(String text) {
        Map<String, Set<String>> result = new HashMap<>();
        patterns.forEach((type, pattern) -> {
            Set<String> names = findMatches(text, pattern);
            if (names != null) {
                result.put(type, names);
            }
        });
        return result;
    }
}
