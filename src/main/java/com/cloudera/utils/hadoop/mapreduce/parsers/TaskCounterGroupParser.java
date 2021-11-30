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

package com.cloudera.utils.hadoop.mapreduce.parsers;

import com.cloudera.utils.hadoop.util.NodeParser;
import org.codehaus.jackson.JsonNode;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by streever on 2016-04-25.
 */
public class TaskCounterGroupParser implements NodeParser {

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
                    rtn.put(groupCounterName+":"+counter.get("name").asText(), counter.get("value").asText());
                }
            }
        } else {
            // WRONG NODE
        }
        return rtn;
    }

    /*
        {
    "jobTaskCounters": {
        "id": "task_1457452103658_20948_m_000000",
        "taskCounterGroup": [
            {
                "counter": [
                    {
                        "name": "FILE_BYTES_READ",
                        "value": 20638097
                    },
                    {
                        "name": "FILE_BYTES_WRITTEN",
                        "value": 356209
                    },
                    {
                        "name": "FILE_READ_OPS",
                        "value": 0
                    },
                    {
                        "name": "FILE_LARGE_READ_OPS",
                        "value": 0
                    },
                    {
                        "name": "FILE_WRITE_OPS",
                        "value": 0
                    },
                    {
                        "name": "HDFS_BYTES_READ",
                        "value": 165536
                    },
                    {
                        "name": "HDFS_BYTES_WRITTEN",
                        "value": 106007
                    },
                    {
                        "name": "HDFS_READ_OPS",
                        "value": 166
                    },
                    {
                        "name": "HDFS_LARGE_READ_OPS",
                        "value": 0
                    },
                    {
                        "name": "HDFS_WRITE_OPS",
                        "value": 129
                    }
                ],
                "counterGroupName": "org.apache.hadoop.mapreduce.FileSystemCounter"
            },
            {
                "counter": [
                    {
                        "name": "MAP_INPUT_RECORDS",
                        "value": 1
                    },
                    {
                        "name": "MAP_OUTPUT_RECORDS",
                        "value": 0
                    },
                    {
                        "name": "SPLIT_RAW_BYTES",
                        "value": 67
                    },
                    {
                        "name": "SPILLED_RECORDS",
                        "value": 0
                    },
                    {
                        "name": "FAILED_SHUFFLE",
                        "value": 0
                    },
                    {
                        "name": "MERGED_MAP_OUTPUTS",
                        "value": 0
                    },
                    {
                        "name": "GC_TIME_MILLIS",
                        "value": 232
                    },
                    {
                        "name": "CPU_MILLISECONDS",
                        "value": 26060
                    },
                    {
                        "name": "PHYSICAL_MEMORY_BYTES",
                        "value": 353914880
                    },
                    {
                        "name": "VIRTUAL_MEMORY_BYTES",
                        "value": 2055172096
                    },
                    {
                        "name": "COMMITTED_HEAP_BYTES",
                        "value": 331350016
                    }
                ],
                "counterGroupName": "org.apache.hadoop.mapreduce.TaskCounter"
            },
            {
                "counter": [
                    {
                        "name": "BYTES_READ",
                        "value": 0
                    }
                ],
                "counterGroupName": "org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter"
            },
            {
                "counter": [
                    {
                        "name": "BYTES_WRITTEN",
                        "value": 0
                    }
                ],
                "counterGroupName": "org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter"
            }
        ]
    }
}

     */
}
