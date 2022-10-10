
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

public class Endpoint {

    /**
     * The hdfs uri for a NON HA Endpoint and HA Endpoint.  When HA is used, the name is the Service name.
     * For non-HA, it needs to be the host:port of the Namenode RPC endpoint.
     *
     */
    private String name;
    /**
     * If HA is enabled for the Endpoint, we need to look at the dfsNameService to determine
     * how to build it, if necessary, and reference it.
     */
    private Boolean haEnabled;
    /**
     * Support HA references.
     */
    private DfsNameService dfsNameService;
    /**
     * Directory for operation.
     */
    private String directory;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getHaEnabled() {
        return haEnabled;
    }

    public void setHaEnabled(Boolean haEnabled) {
        this.haEnabled = haEnabled;
    }

    public DfsNameService getDfsNameService() {
        return dfsNameService;
    }

    public void setDfsNameService(DfsNameService dfsNameService) {
        this.dfsNameService = dfsNameService;
    }

    public String getDirectory() {
        if (directory == null)
            throw new RuntimeException("Directory needs to be specified");
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "name='" + name + '\'' +
                ", haEnabled=" + haEnabled +
                ", dfsNameService=" + dfsNameService +
                ", directory='" + directory + '\'' +
                '}';
    }
}
