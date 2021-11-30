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
