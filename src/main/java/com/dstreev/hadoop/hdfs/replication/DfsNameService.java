package com.dstreev.hadoop.hdfs.replication;

import java.util.List;

/**
 * Created by dstreev on 2016-04-05.
 */
public class DfsNameService {
    /*
        "dfsNameservice" :
    {
      "defined":false,
      "name":"REPLICA",
      "namenodeHosts": [ {
        "fqdn": "nnhost1.somewhere.com",
        "rpcPort":8020,
        "serviceRpcPort":52231,
        "httpAddress":50070,
        "httpsAddress":50071
      } , {
        "fqdn": "nnhost2.somewhere.com",
        "rpcPort":8020,
        "serviceRpcPort":52231,
        "httpAddress":50070,
        "httpsAddress":50071

      }]
    },

     */

    /**
     * Identifies if the HDFS Endpoint is defined in the supplied configurations.  This is required when the HDFS Endpoint is HA.  If it is HA and NOT defined, we need to build it and add it to the current configuration.
     */
    private Boolean defined;
    /**
     * The list of hosts that will be used to construct the HA dfs.nameservice.
     */
    private List<NamenodeHost> namenodeHosts;

    public List<NamenodeHost> getNamenodeHosts() {
        return namenodeHosts;
    }

    public Boolean getDefined() {

        return defined;
    }

    public void setDefined(Boolean defined) {
        this.defined = defined;
    }

    @Override
    public String toString() {
        return "DfsNameService{" +
                "defined=" + defined +
                ", namenodeHosts=" + namenodeHosts +
                '}';
    }
}
