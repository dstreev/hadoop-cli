package com.dstreev.hadoop.hdfs.replication;

/**
 * Created by dstreev on 2016-04-06.
 */
public class SyncStats {
    /*
        "success": true,
    "application_id": "xxxxxx",
    "initial_load": false,
    "snapshot_begin": ".snapshot-2016-04-04_023143",
    "snapshot_end": ".snapshot-2016-04-04_043143",
    "runtime": {
      "started": "xxxxx",
      "finished": "xxxxx",
      "duration": "xxxxx"
    }
*/

    private boolean successful;
    private String applicationId;
    private boolean initialLoad;
    private String snapshotWatermark;
    private String snapshotUpto;

    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public boolean isInitialLoad() {
        return initialLoad;
    }

    public void setInitialLoad(boolean initialLoad) {
        this.initialLoad = initialLoad;
    }

    public String getSnapshotWatermark() {
        return snapshotWatermark;
    }

    public void setSnapshotWatermark(String snapshotWatermark) {
        this.snapshotWatermark = snapshotWatermark;
    }

    public String getSnapshotUpto() {
        return snapshotUpto;
    }

    public void setSnapshotUpto(String snapshotUpto) {
        this.snapshotUpto = snapshotUpto;
    }

    @Override
    public String toString() {
        return "SyncStats{" +
                "successful=" + successful +
                ", applicationId='" + applicationId + '\'' +
                ", initialLoad=" + initialLoad +
                ", snapshotWatermark='" + snapshotWatermark + '\'' +
                ", snapshotUpto='" + snapshotUpto + '\'' +
                '}';
    }
}
