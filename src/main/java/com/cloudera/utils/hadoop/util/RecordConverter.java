/*
 * Copyright (c) 2022. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.cloudera.utils.hadoop.util;

import com.cloudera.utils.hadoop.AbstractStats;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by streever on 2016-04-25.
 */
public class RecordConverter {

    private ObjectMapper mapper = null;
    private JsonNode root = null;
    private Boolean header = Boolean.FALSE;

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

    public static String mapToRecord(String[] fields, Map<String, Object> map, String inDelimiter) {
        // Process Record.
        StringBuilder sb = new StringBuilder();

        String delimiter = null;
        if (inDelimiter != null) {
            delimiter = inDelimiter;
        } else {
            delimiter = AbstractStats.DEFAULT_DELIMITER;
        }

//        Iterator<Map.Entry<String, Object>> entries = map.entrySet().iterator();
        boolean init = false;
        for (String field : fields) {
            Object value = map.get(field);
            if (value != null) {
                try {
                    String strValue = decode(value.toString());
//                    String strValue = URLDecoder.decode(value.toString(), StandardCharsets.UTF_8.toString());
                    sb.append(strValue);
                } catch (Throwable t) {
                    System.err.println("(App)Issue with decode: " + field + ":" + value);
                }
                if (!field.equals(fields[fields.length - 1])) {
                    sb.append(delimiter);
                }
            } else {
                if (!field.equals(fields[fields.length - 1])) {
                    sb.append(delimiter);
                }
            }
        }
        return sb.toString();
    }

    protected Map<String, Object> buildInnerRecord(Map<String, Object> record, StringBuilder
            treeHierarchy, String key, JsonNode node, TraversePath traversePath) {

        Map<String, Object> rtn = null;
        if (record != null)
            rtn = record;
        else
            rtn = new LinkedHashMap<String, Object>();

        if (node.isValueNode()) {
            try {
//                rtn.put(key, URLEncoder.encode(node.asText(), "UTF-8"));
                rtn.put(key, decode(node.asText()));
            } catch (UnsupportedEncodingException e) {
                System.err.println("(App) Issue with decode: " + key + ":" + node.asText());
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
            Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
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

    public static String decode(String value) throws UnsupportedEncodingException {
        String rtn = value;
        String checked = URLDecoder.decode(value, StandardCharsets.UTF_8.toString()).replaceAll("\n", " ");
        rtn = checked;
        return rtn;
    }
}
