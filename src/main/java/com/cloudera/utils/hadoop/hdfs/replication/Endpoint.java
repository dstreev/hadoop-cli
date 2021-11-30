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
