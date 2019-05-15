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

package com.streever.hadoop.hdfs.replication;


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
