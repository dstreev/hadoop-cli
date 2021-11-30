/*
 *  Hadoop CLI
 *
 *  (c) 2016-2019 David W. Streever. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with David W. Streever, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with David W. Streever or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) David W. Streever PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) David W. Streever DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) David W. Streever IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 *  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, David W. Streever IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 *
 */

package com.cloudera.utils.hadoop.hdfs.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

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
