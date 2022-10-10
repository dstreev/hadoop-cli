
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
