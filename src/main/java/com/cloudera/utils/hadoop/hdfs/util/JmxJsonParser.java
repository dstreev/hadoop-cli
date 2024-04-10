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

package com.cloudera.utils.hadoop.hdfs.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by streever on 2016-03-21.
 */
public class JmxJsonParser {

    private ObjectMapper mapper = null;
    private JsonNode root = null;
    private JsonNode beanArrayNode = null;

    private static String NAME = "name";

    private Map<String, Map<String, Object>> beanMap = new TreeMap<String, Map<String,Object>>();

    public JmxJsonParser(String input) throws Exception {
        mapper = new ObjectMapper();
        try {
            root = mapper.readValue(input, JsonNode.class);
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
                    Iterator<Map.Entry<String, JsonNode>> iEntries = beanNode.fields();

                    while (iEntries.hasNext()) {
                        Map.Entry<String, JsonNode> entry = iEntries.next();

                        if (entry.getValue().isNumber()) {
                            content.put(entry.getKey(), entry.getValue().numberValue());
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
