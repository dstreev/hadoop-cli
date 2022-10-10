
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
