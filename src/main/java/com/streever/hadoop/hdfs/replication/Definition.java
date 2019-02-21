package com.streever.hadoop.hdfs.replication;

/**
 * Created by streever on 2016-04-05.
 */
public class Definition {

    private String name;
    private String stateOutputDirectory;

    private SnapshotNamePattern snapshotNamePattern;

    private Endpoint source;
    private Endpoint target;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStateOutputDirectory() {
        return stateOutputDirectory;
    }

    public void setStateOutputDirectory(String stateOutputDirectory) {
        this.stateOutputDirectory = stateOutputDirectory;
    }

    public SnapshotNamePattern getSnapshotNamePattern() {
        return snapshotNamePattern;
    }

    public void setSnapshotNamePattern(SnapshotNamePattern snapshotNamePattern) {
        this.snapshotNamePattern = snapshotNamePattern;
    }

    public Endpoint getSource() {
        return source;
    }

    public void setSource(Endpoint source) {
        this.source = source;
    }

    public Endpoint getTarget() {
        return target;
    }

    public void setTarget(Endpoint target) {
        this.target = target;
    }

    @Override
    public String toString() {
        return "Definition{" +
                "name='" + name + '\'' +
                ", stateOutputDirectory='" + stateOutputDirectory + '\'' +
                ", snapshotNamePattern=" + snapshotNamePattern +
                ", source=" + source +
                ", target=" + target +
                '}';
    }
}
