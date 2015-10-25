package edu.usc.cs.ir.cwork.solr;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tg on 10/25/15.
 */
public class SolrUtil {

    public static final String MD_SUFFIX = "_md";
    public static final String MULTIVAL_SUFFIX = "s";
    //type map
    public static final Map<Class, String> typeSuffix = new HashMap<>();
    static {
        typeSuffix.put(Integer.class, "_i");
        typeSuffix.put(Boolean.class, "_b");
        typeSuffix.put(Long.class, "_l");
        typeSuffix.put(Float.class, "_f");
        typeSuffix.put(Double.class, "_d");
        typeSuffix.put(String.class, "_s");
        typeSuffix.put(Date.class, "_dt");    //time
    }


    public static ContentBean createBean(Content content){

        ContentBean bean = new ContentBean();
        bean.setContentType(content.getContentType());
        bean.setUrl(content.getUrl());

        Map<String, Object> mdFields = new HashMap<>();
        bean.setMetadata(mdFields);

        Metadata metadata = content.getMetadata();
        String[] names = metadata.names();

        for (String name : names) {
            boolean multivalued = metadata.isMultiValued(name);
            Serializable val = multivalued ? metadata.getValues(name)
                                           : metadata.get(name);
            //TODO: convert string to proper type
            Class<?> valType = metadata.get(name).getClass();
            String dynFieldName = getDynamicFieldName(name, valType, multivalued);
            mdFields.put(dynFieldName, val);
        }
        return bean;
    }

    private static String getDynamicFieldName(String keyName, Class<?> valType, boolean multivalued) {
        if (!typeSuffix.containsKey(valType)){
            throw new IllegalStateException("Type Not mapped :" + valType);
        }

        //md_name_i[?s]
        StringBuilder sb = new StringBuilder();
        sb.append(keyName.trim().toLowerCase())
                .append(typeSuffix.get(valType));
        if (multivalued) {
            sb.append(MULTIVAL_SUFFIX);
        }
        sb.append(MD_SUFFIX);
        return sb.toString();
    }
}
