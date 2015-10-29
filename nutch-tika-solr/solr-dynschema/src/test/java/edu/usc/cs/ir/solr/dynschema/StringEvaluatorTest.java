package edu.usc.cs.ir.solr.dynschema;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by tg on 10/28/15.
 */
public class StringEvaluatorTest {

    @Test
    public void testValuate() throws Exception {
        StringEvaluator valuator = new StringEvaluator();
        assertEquals(1234, valuator.valueOf("1234"));
        long l = (long)Integer.MAX_VALUE + 1;
        assertEquals(l, valuator.valueOf(l + ""));

        assertEquals(true, valuator.valueOf("true"));
        assertEquals(true, valuator.valueOf("True"));
        assertEquals(true, valuator.valueOf("TRUE"));
        assertEquals(false, valuator.valueOf("False"));
        assertEquals(false, valuator.valueOf("false"));
        assertEquals(false, valuator.valueOf("FALSE"));
        assertEquals(0.0, valuator.valueOf("0.0"));
        assertEquals(+1.4, valuator.valueOf("+1.4"));
        assertEquals(-1.4, valuator.valueOf("-1.4"));
        assertEquals(2.0, valuator.valueOf("2.0"));

    }
}