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

package com.streever.hadoop.mapreduce.parsers;

import com.streever.hadoop.util.NodeParser;
import org.codehaus.jackson.JsonNode;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by streever on 2016-04-25.
 */
public class JobCounterGroupParser implements NodeParser {

    @Override
    public Map<String, String> parse(JsonNode node) {
        Map<String, String> rtn = new LinkedHashMap<String, String>();
        if (node.isArray()) {
            Iterator<JsonNode> nodes = node.getElements();
            while (nodes.hasNext()) {
                JsonNode groupCounterNode = nodes.next();
                String groupCounterName = groupCounterNode.get("counterGroupName").asText();
                Iterator<JsonNode> counters = groupCounterNode.get("counter").getElements();
                while (counters.hasNext()) {
                    JsonNode counter = counters.next();
                    rtn.put(groupCounterName+":"+counter.get("name").asText() + ":map", counter.get("mapCounterValue").asText());
                    rtn.put(groupCounterName+":"+counter.get("name").asText() + ":reduce", counter.get("reduceCounterValue").asText());
                    rtn.put(groupCounterName+":"+counter.get("name").asText() + ":total", counter.get("totalCounterValue").asText());
                }
            }
        } else {
            // WRONG NODE
        }
        return rtn;
    }

    /*
    {
  "jobCounters": {
    "id": "job_1457452103658_20948",
    "counterGroup": [
      {
        "counter": [
          {
            "name": "FILE_BYTES_READ",
            "totalCounterValue": 20638097,
            "reduceCounterValue": 0,
            "mapCounterValue": 20638097
          },
          {
            "name": "FILE_BYTES_WRITTEN",
            "totalCounterValue": 356209,
            "reduceCounterValue": 0,
            "mapCounterValue": 356209
          },
          {
            "name": "FILE_READ_OPS",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "FILE_LARGE_READ_OPS",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "FILE_WRITE_OPS",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "HDFS_BYTES_READ",
            "totalCounterValue": 165536,
            "reduceCounterValue": 0,
            "mapCounterValue": 165536
          },
          {
            "name": "HDFS_BYTES_WRITTEN",
            "totalCounterValue": 106007,
            "reduceCounterValue": 0,
            "mapCounterValue": 106007
          },
          {
            "name": "HDFS_READ_OPS",
            "totalCounterValue": 166,
            "reduceCounterValue": 0,
            "mapCounterValue": 166
          },
          {
            "name": "HDFS_LARGE_READ_OPS",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "HDFS_WRITE_OPS",
            "totalCounterValue": 129,
            "reduceCounterValue": 0,
            "mapCounterValue": 129
          }
        ],
        "counterGroupName": "org.apache.hadoop.mapreduce.FileSystemCounter"
      },
      {
        "counter": [
          {
            "name": "TOTAL_LAUNCHED_MAPS",
            "totalCounterValue": 1,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "OTHER_LOCAL_MAPS",
            "totalCounterValue": 1,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "SLOTS_MILLIS_MAPS",
            "totalCounterValue": 148202,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "MILLIS_MAPS",
            "totalCounterValue": 74101,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "VCORES_MILLIS_MAPS",
            "totalCounterValue": 74101,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "MB_MILLIS_MAPS",
            "totalCounterValue": 113819136,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          }
        ],
        "counterGroupName": "org.apache.hadoop.mapreduce.JobCounter"
      },
      {
        "counter": [
          {
            "name": "MAP_INPUT_RECORDS",
            "totalCounterValue": 1,
            "reduceCounterValue": 0,
            "mapCounterValue": 1
          },
          {
            "name": "MAP_OUTPUT_RECORDS",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "SPLIT_RAW_BYTES",
            "totalCounterValue": 67,
            "reduceCounterValue": 0,
            "mapCounterValue": 67
          },
          {
            "name": "SPILLED_RECORDS",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "FAILED_SHUFFLE",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "MERGED_MAP_OUTPUTS",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          },
          {
            "name": "GC_TIME_MILLIS",
            "totalCounterValue": 232,
            "reduceCounterValue": 0,
            "mapCounterValue": 232
          },
          {
            "name": "CPU_MILLISECONDS",
            "totalCounterValue": 26060,
            "reduceCounterValue": 0,
            "mapCounterValue": 26060
          },
          {
            "name": "PHYSICAL_MEMORY_BYTES",
            "totalCounterValue": 353914880,
            "reduceCounterValue": 0,
            "mapCounterValue": 353914880
          },
          {
            "name": "VIRTUAL_MEMORY_BYTES",
            "totalCounterValue": 2055172096,
            "reduceCounterValue": 0,
            "mapCounterValue": 2055172096
          },
          {
            "name": "COMMITTED_HEAP_BYTES",
            "totalCounterValue": 331350016,
            "reduceCounterValue": 0,
            "mapCounterValue": 331350016
          }
        ],
        "counterGroupName": "org.apache.hadoop.mapreduce.TaskCounter"
      },
      {
        "counter": [
          {
            "name": "BYTES_READ",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          }
        ],
        "counterGroupName": "org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter"
      },
      {
        "counter": [
          {
            "name": "BYTES_WRITTEN",
            "totalCounterValue": 0,
            "reduceCounterValue": 0,
            "mapCounterValue": 0
          }
        ],
        "counterGroupName": "org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter"
      }
    ]
  }
}

     */
}
