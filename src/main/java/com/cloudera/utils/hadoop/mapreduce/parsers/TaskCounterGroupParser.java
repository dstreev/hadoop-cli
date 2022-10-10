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
