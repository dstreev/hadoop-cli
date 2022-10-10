
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

import java.util.List;

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
