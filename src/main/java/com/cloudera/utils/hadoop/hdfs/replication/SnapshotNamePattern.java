
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

package com.cloudera.utils.hadoop.hdfs.replication;

/**
 * When we take snapshots to support this process, we can use the default entries
 * or define a convention that matches our need/desire.
 *
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
