package com.streever.hadoop.hdfs.replication;

import java.util.List;

/**
 * Created by streever on 2016-04-06.
 */
public class State {

    private SyncStats current;
    private List<SyncStats> last10;


    public SyncStats getCurrent() {
        return current;
    }

    public void setCurrent(SyncStats current) {
        this.current = current;
    }

    public List<SyncStats> getLast10() {
        return last10;
    }

    public void setLast10(List<SyncStats> last10) {
        this.last10 = last10;
    }

    @Override
    public String toString() {
        return "State{" +
                "current=" + current +
                ", last10=" + last10 +
                '}';
    }
}
