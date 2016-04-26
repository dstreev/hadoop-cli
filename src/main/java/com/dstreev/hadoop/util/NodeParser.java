package com.dstreev.hadoop.util;

import org.codehaus.jackson.JsonNode;

import java.util.Map;

/**
 * Created by dstreev on 2016-04-25.
 */
public interface NodeParser {
    Map<String, String> parse(JsonNode node);
}
