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


public class NamenodeHost {
    /*
          "namenodeHosts": [ {
        "fqdn": "nnhost1.somewhere.com",
        "rpcPort":8020,
        "serviceRpcPort":52231,
        "httpPort":50070,
        "httpsPort":50071
      } , {

     */

    /**
     * The Fully Qualified Domain Name of the Namenode that will be used to construct the dfs.nameservice.
     */
    private String fqdn;
    /**
     * The RPC Port used for the service on the host.
     */
    private Integer rpcPort;
    /**
     * The Service RPC Post used for the service on the host.
     */
    private Integer serviceRpcPort;
    /**
     * The http port for the Namenode UI.
     */
    private String httpPort;
    /**
     * The https port for the Namenode UI.
     */
    private String httpsPort;

    public String getFqdn() {
        return fqdn;
    }

    public void setFqdn(String fqdn) {
        this.fqdn = fqdn;
    }

    public Integer getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(Integer rpcPort) {
        this.rpcPort = rpcPort;
    }

    public Integer getServiceRpcPort() {
        return serviceRpcPort;
    }

    public void setServiceRpcPort(Integer serviceRpcPort) {
        this.serviceRpcPort = serviceRpcPort;
    }

    public String getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(String httpPort) {
        this.httpPort = httpPort;
    }

    public String getHttpsPort() {
        return httpsPort;
    }

    public void setHttpsPort(String httpsPort) {
        this.httpsPort = httpsPort;
    }

    @Override
    public String toString() {
        return "NamenodeHost{" +
                "fqdn='" + fqdn + '\'' +
                ", rpcPort=" + rpcPort +
                ", serviceRpcPort=" + serviceRpcPort +
                ", httpPort='" + httpPort + '\'' +
                ", httpsPort='" + httpsPort + '\'' +
                '}';
    }
}
