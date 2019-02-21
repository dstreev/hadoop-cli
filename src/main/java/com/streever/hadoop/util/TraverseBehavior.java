package com.streever.hadoop.util;

/**
 * Created by streever on 2016-04-25.
 */
public class TraverseBehavior {
    public enum TRAVERSE_MODE {
        EXPLODE,
        FLATTEN
    }

    private TRAVERSE_MODE mode;
    private NodeParser nodeParser;

    public TraverseBehavior(TRAVERSE_MODE mode, NodeParser nodeParser) {
        this.mode = mode;
        this.nodeParser = nodeParser;
    }

    public TRAVERSE_MODE getMode() {
        return mode;
    }

    public NodeParser getNodeParser() {
        return nodeParser;
    }
}
