package edu.usc.cs.ir.cwork.ner;

import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by tg on 11/7/15.
 */
public class RegexNERecogniserTest {

    @Test
    public void testRecognise() throws Exception {

        RegexNERecogniser instance = RegexNERecogniser.getInstance();
        Map<String, Set<String>> map = instance.recognise("I am not looking for a pistol and AK-47 and ak-47");
        assertTrue(map.get("WEAPON_TYPE").contains("pistol"));
        assertTrue(map.get("WEAPON_NAME").contains("ak-47"));
        assertTrue(map.get("WEAPON_NAME").contains("AK-47"));
    }
}