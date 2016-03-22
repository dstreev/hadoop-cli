package com.dstreev.hadoop.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by dstreev on 2016-03-21.
 */
public class JmxJsonParser {

    private ObjectMapper mapper = null;
    private JsonNode root = null;
    private JsonNode beanArrayNode = null;

    private static String NAME = "name";

    private Map<String, Map<String, Object>> beanMap = new TreeMap<String, Map<String,Object>>();

    public JmxJsonParser(InputStream inputStream) throws Exception {
        mapper = new ObjectMapper();
        try {
            root = mapper.readValue(inputStream, JsonNode.class);
            beanArrayNode = root.get("beans");
            if (beanArrayNode == null) {
                throw new Exception("Couldn't locate Jmx 'Beans' array.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Map<String, Object> getJmxBeanContent(String name) {
        Map<String, Object> rtn = null;
        rtn = beanMap.get(name);
        if (rtn == null) {
            for (JsonNode beanNode : beanArrayNode) {
                if (beanNode.get(NAME).asText().equals(name)) {
                    Map<String, Object> content = new TreeMap<String, Object>();
//                    Iterator<JsonNode> iNodes = beanNode.iterator();
                    Iterator<Map.Entry<String, JsonNode>> iEntries = beanNode.getFields();

                    while (iEntries.hasNext()) {
                        Map.Entry<String, JsonNode> entry = iEntries.next();

                        if (entry.getValue().isNumber()) {
                            content.put(entry.getKey(), entry.getValue().getNumberValue());
                        } else {
                            content.put(entry.getKey(), entry.getValue().asText());
                        }
                    }
                    beanMap.put(name, content);
                    rtn = content;
                    break;
                }
            }
        }
        return rtn;
    }
}
