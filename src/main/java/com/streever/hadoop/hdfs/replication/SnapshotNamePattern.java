package com.streever.hadoop.hdfs.replication;

/**
 * When we take snapshots to support this process, we can use the default entries
 * or define a convention that matches our need/desire.
 *
 *
 * Created by David Streever on 2016-04-05.
 */
public class SnapshotNamePattern {
    /*
      "snapshotNamePattern" : {
    "use.default.names" : false,
    "name.date.pattern" : "yyyyMMdd24Hmmss"
  },

     */

    /**
     * Use the default snapshot naming conventions when building the snapshots.
     */
    private Boolean useDefault;
    /**
     * When defaults aren't used, this is the pattern you'll use to name the snapshot.
     *
     * This pattern needs to be a valid SimpleDateFormat pattern.
     */
    private String namePattern;

    public Boolean getUseDefault() {
        return useDefault;
    }

    public void setUseDefault(Boolean useDefault) {
        this.useDefault = useDefault;
    }

    public String getNamePattern() {
        return namePattern;
    }

    public void setNamePattern(String namePattern) {
        this.namePattern = namePattern;
    }

    @Override
    public String toString() {
        return "SnapshotNamePattern{" +
                "useDefault=" + useDefault +
                ", namePattern='" + namePattern + '\'' +
                '}';
    }
}
