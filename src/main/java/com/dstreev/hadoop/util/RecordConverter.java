package com.dstreev.hadoop.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by dstreev on 2016-04-25.
 */
public class RecordConverter {

    private ObjectMapper mapper = null;
    private JsonNode root = null;
    private Boolean header = Boolean.FALSE;

    private static final String delimiter = "\u0001"; // cntrl-a

    public RecordConverter() {
        mapper = new ObjectMapper();
    }


    public Map<String, Object> convert(Map<String, Object> parent, String nodeStr, String key, TraversePath traversePath) throws IOException {

        JsonNode startNode = null;

        JsonNode root = mapper.readValue(nodeStr, JsonNode.class);

        Map<String, Object> rtn = null;
        if (parent != null)
            rtn = parent;
        else
            rtn = new LinkedHashMap<String, Object>();

        if (key != null) {
            startNode = root.get(key);
            rtn = buildInnerRecord(rtn, null, key, startNode, traversePath);
        } else {
            startNode = root;
            rtn = buildInnerRecord(rtn, null, key, startNode, traversePath);
        }

        return rtn;
    }

    public Map<String, Object> convert(Map<String, Object> parent, JsonNode node, String key, TraversePath traversePath) throws IOException {

        JsonNode startNode = null;

        JsonNode root = node;

        Map<String, Object> rtn = null;
        if (parent != null)
            rtn = parent;
        else
            rtn = new LinkedHashMap<String, Object>();

        if (key != null) {
            startNode = root.get(key);
            rtn = buildInnerRecord(rtn, null, key, startNode, traversePath);
        } else {
            startNode = root;
            rtn = buildInnerRecord(rtn, null, key, startNode, traversePath);
        }

        return rtn;
    }

    public static String mapToRecord(Map<String, Object> map, boolean header, String inDelimiter) {
        // Process Record.
        StringBuilder sb = new StringBuilder();

        String delimiter = null;
        if (inDelimiter != null) {
            delimiter = inDelimiter;
        } else {
            delimiter = RecordConverter.delimiter;
        }

        Iterator<Map.Entry<String, Object>> entries = map.entrySet().iterator();
        boolean init = false;
        while (entries.hasNext()) {
            Map.Entry<String, Object> entry = entries.next();
            if (header) {
                if (init)
                    sb.append(delimiter);
                sb.append(entry.getKey());
            } else {
                if (init)
                    sb.append(delimiter);
                sb.append(entry.getValue());
            }
            init = true;
        }
        return sb.toString();
    }

    protected Map<String, Object> buildInnerRecord(Map<String, Object> record, StringBuilder treeHierarchy, String key, JsonNode
            node, TraversePath traversePath) {

        Map<String, Object> rtn = null;
        if (record != null)
            rtn = record;
        else
            rtn = new LinkedHashMap<String, Object>();

        if (node.isValueNode()) {
            try {
                rtn.put(key, URLEncoder.encode(node.asText(),"UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } else if (node.isArray()) {
            if (treeHierarchy == null) {
                treeHierarchy = new StringBuilder();
                treeHierarchy.append(key);
            } else {
                treeHierarchy.append(".").append(key);
            }
            if (traversePath != null) {
                if (traversePath.getPaths().containsKey(treeHierarchy.toString())) {
                    TraverseBehavior behavior = traversePath.getPaths().get(treeHierarchy.toString());
                    NodeParser parser = behavior.getNodeParser();
                    Map<String, String> elementMap = parser.parse(node);
                    rtn.putAll(elementMap);
                }
            }
        } else if (node.isContainerNode()) {
            Iterator<Map.Entry<String, JsonNode>> iter = node.getFields();
            if (treeHierarchy == null) {
                treeHierarchy = new StringBuilder();
                treeHierarchy.append(key);
            } else {
                treeHierarchy.append(".").append(key);
            }
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> val = iter.next();

                buildInnerRecord(rtn, treeHierarchy, val.getKey(), val.getValue(), traversePath);
            }
        }

        return rtn;
    }

}
