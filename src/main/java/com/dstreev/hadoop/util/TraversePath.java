package com.dstreev.hadoop.util;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by dstreev on 2016-04-25.
 */
public class TraversePath {

    private Map<String, TraverseBehavior> paths = new TreeMap<String, TraverseBehavior>();

    public TraversePath() {

    }

    public void addPath(String pathKey, TraverseBehavior path) {
        paths.put(pathKey, path);
    }

    public Map<String, TraverseBehavior> getPaths() {
        return paths;
    }

    public void clear() {
        paths = new TreeMap<String, TraverseBehavior>();
    }

}
