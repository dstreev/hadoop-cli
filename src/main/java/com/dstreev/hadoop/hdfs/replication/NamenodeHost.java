package com.dstreev.hadoop.hdfs.replication;

/**
 * Created by dstreev on 2016-04-05.
 */
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
