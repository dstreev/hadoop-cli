package com.dstreev.hadoop.mapreduce;

import com.dstreev.hadoop.mapreduce.parsers.JobCounterGroupParser;
import com.dstreev.hadoop.mapreduce.parsers.TaskCounterGroupParser;
import com.dstreev.hadoop.util.RecordConverter;
import com.dstreev.hadoop.util.TraverseBehavior;
import com.dstreev.hadoop.util.TraversePath;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by dstreev on 2016-04-25.
 */
public class MRJobRecordConverter {

    private ObjectMapper mapper = null;
    private RecordConverter recordConverter = null;

    public MRJobRecordConverter() {
        mapper = new ObjectMapper();
        recordConverter = new RecordConverter();
    }

    /*
    {
    "jobs": {
        "job": [
            {
                "reducesTotal": 0,
                "reducesCompleted": 0,
                "name": "oozie:launcher:T=hive:W=Hive QL - Markets_Bloomber",
                "state": "SUCCEEDED",
                "submitTime": 1461015604472,
                "id": "job_1457452103658_18275",
                "user": "wre",
                "queue": "default",
                "startTime": 1461015611210,
                "finishTime": 1461015663008,
                "mapsTotal": 1,
                "mapsCompleted": 1
            },
            {
                "reducesTotal": 0,
                "reducesCompleted": 0,
                "name": "oozie:launcher:T=hive:W=Hive QL - Markets_Bloomber",
                "state": "SUCCEEDED",
                "submitTime": 1461015663876,
                "id": "job_1457452103658_18277",
                "user": "wre",
                "queue": "default",
                "startTime": 1461015670193,
                "finishTime": 1461015718287,
                "mapsTotal": 1,
                "mapsCompleted": 1
            }
        ]
    }
}

     */
    public List<String> jobIdList(String jobsJson) throws IOException {
        List<String> rtn = new ArrayList<String>();
        //System.out.println(jobsJson);
        JsonNode jobs = mapper.readValue(jobsJson, JsonNode.class);

        if (jobs != null) {
            JsonNode jobsNode = jobs.get("jobs");
            if (jobsNode != null) {

                JsonNode jobsArrayNode = jobsNode.get("job");

                if (jobsArrayNode != null) {

                    if (jobsArrayNode.isArray()) {
                        Iterator<JsonNode> jobIter = jobsArrayNode.getElements();
                        while (jobIter.hasNext()) {
                            JsonNode jobNode = jobIter.next();
                            rtn.add(jobNode.get("id").asText());
                        }
                    } else {
                        System.out.println("Nope");
                    }
                }
            }
        }
        return rtn;
    }

    /*
    {
    "job": {
        "failedMapAttempts": 0,
        "avgShuffleTime": 0,
        "successfulMapAttempts": 1,
        "uberized": false,
        "reducesTotal": 0,
        "reducesCompleted": 0,
        "killedMapAttempts": 0,
        "failedReduceAttempts": 0,
        "name": "oozie:launcher:T=hive:W=Hive QL - DIMA_Masters_Pos_n_Trans:A=step3:ID=0005312-160130000925352-oozie-oozi-W",
        "avgMapTime": 74101,
        "avgReduceTime": 0,
        "state": "SUCCEEDED",
        "avgMergeTime": 0,
        "submitTime": 1461587936771,
        "id": "job_1457452103658_20948",
        "user": "wre",
        "killedReduceAttempts": 0,
        "queue": "default",
        "successfulReduceAttempts": 0,
        "startTime": 1461587943444,
        "finishTime": 1461588020358,
        "mapsTotal": 1,
        "diagnostics": "",
        "mapsCompleted": 1
    }
}
     */
    public Map<String, String> jobDetail(String jobJson) throws IOException {

        Map<String, String> rtn = recordConverter.convert(null, jobJson, "job", null);

        return rtn;
    }

    /*
    {
    "tasks": {
        "task": [
            {
                "elapsedTime": 74101,
                "state": "SUCCEEDED",
                "type": "MAP",
                "progress": 100,
                "id": "task_1457452103658_20948_m_000000",
                "startTime": 1461587946205,
                "finishTime": 1461588020306,
                "successfulAttempt": "attempt_1457452103658_20948_m_000000_0"
            }
        ]
    }
}
     */
    public List<String> jobTaskList(String tasksJson) throws IOException {
        List<String> rtn = new ArrayList<String>();
        //System.out.println(tasksJson);
        JsonNode jobs = mapper.readValue(tasksJson, JsonNode.class);

        if (jobs != null) {
            JsonNode tasksNode = jobs.get("tasks");
            if (tasksNode != null) {
                JsonNode tasksArrayNode = tasksNode.get("task");

                if (tasksArrayNode != null) {
                    if (tasksArrayNode.isArray()) {
                        Iterator<JsonNode> taskIter = tasksArrayNode.getElements();
                        while (taskIter.hasNext()) {
                            JsonNode taskNode = taskIter.next();
                            rtn.add(taskNode.get("id").asText());
                        }
                    } else {
                        System.out.println("Nope");
                    }
                }
            }
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
    public Map<String, String> jobCounters(String counterJson) {
        Map<String, String> rtn = null;

        TraversePath tp = new TraversePath();
        TraverseBehavior tbcg = new TraverseBehavior(TraverseBehavior.TRAVERSE_MODE.FLATTEN, new JobCounterGroupParser());
        tp.addPath("jobCounters.counterGroup", tbcg);

        try {
            rtn = recordConverter.convert(null, counterJson, "jobCounters", tp);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rtn;
    }

    /*
    {
    "task": {
        "elapsedTime": 74101,
        "state": "SUCCEEDED",
        "type": "MAP",
        "progress": 100,
        "id": "task_1457452103658_20948_m_000000",
        "startTime": 1461587946205,
        "finishTime": 1461588020306,
        "successfulAttempt": "attempt_1457452103658_20948_m_000000_0"
    }
}
     */
    public Map<String, String> taskDetail(String jobId, String taskJson) throws IOException {
        Map<String, String> rtn = new LinkedHashMap<String, String>();
        rtn.put("jobId", jobId);

        rtn = recordConverter.convert(rtn, taskJson, "task", null);

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
    public Map<String, String> taskCounter(String jobId, String counterJson) {
        Map<String, String> rtn = new LinkedHashMap<String, String>();
        rtn.put("jobId", jobId);

        TraversePath tp = new TraversePath();
        TraverseBehavior tbcg = new TraverseBehavior(TraverseBehavior.TRAVERSE_MODE.FLATTEN, new TaskCounterGroupParser());
        tp.addPath("jobTaskCounters.taskCounterGroup", tbcg);
        try {
            rtn = recordConverter.convert(rtn, counterJson, "jobTaskCounters", tp);

        } catch (IOException ioe) {

        }
        return rtn;
    }

    /*
{
    "taskAttemptList": {
        "taskAttempt": [
            {
                "status": "map",
                "nodeHttpAddress": "ip-172-31-12-220.ec2.internal:8042",
                "assignedContainerId": "container_1457452103658_20948_01_000002",
                "elapsedTime": 74101,
                "state": "SUCCEEDED",
                "type": "MAP",
                "progress": 100,
                "id": "attempt_1457452103658_20948_m_000000_0",
                "rack": "/default-rack",
                "startTime": 1461587946205,
                "finishTime": 1461588020306,
                "diagnostics": ""
            }
        ]
    }
}
     */
    public List<String> taskAttemptList(String attemptsJson) throws IOException {
        List<String> rtn = new ArrayList<String>();
        //System.out.println(attemptsJson);
        JsonNode jobs = mapper.readValue(attemptsJson, JsonNode.class);

        if (jobs != null) {
            JsonNode attemptsNode = jobs.get("taskAttemptList");
            if (attemptsNode != null) {

                JsonNode attemptsArrayNode = attemptsNode.get("taskAttempt");

                if (attemptsArrayNode != null) {

                    if (attemptsArrayNode.isArray()) {
                        Iterator<JsonNode> attemptIter = attemptsArrayNode.getElements();
                        while (attemptIter.hasNext()) {
                            JsonNode attemptNode = attemptIter.next();
                            rtn.add(attemptNode.get("id").asText());
                        }
                    } else {
                        System.out.println("Nope");
                    }
                }
            }
        }
        return rtn;
    }

    /*
    {
    "taskAttempt": {
        "status": "map",
        "nodeHttpAddress": "ip-172-31-12-220.ec2.internal:8042",
        "assignedContainerId": "container_1457452103658_20948_01_000002",
        "elapsedTime": 74101,
        "state": "SUCCEEDED",
        "type": "MAP",
        "progress": 100,
        "id": "attempt_1457452103658_20948_m_000000_0",
        "rack": "/default-rack",
        "startTime": 1461587946205,
        "finishTime": 1461588020306,
        "diagnostics": ""
    }
}
     */
    public Map<String, String> attemptDetail(String jobId, String taskId, String attemptJson) throws IOException {
        Map<String, String> rtn = new LinkedHashMap<String, String>();
        rtn.put("jobId", jobId);
        rtn.put("taskId", taskId);

        rtn = recordConverter.convert(rtn, attemptJson, "taskAttempt", null);

        return rtn;
    }

    /*

    {
    "jobTaskAttemptCounters": {
        "taskAttemptCounterGroup": [
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
        ],
        "id": "attempt_1457452103658_20948_m_000000_0"
    }
}
     */
    public Map<String,String> attemptCounter(String jobId, String taskId, String counterJson) throws IOException {
        Map<String,String> rtn = new LinkedHashMap<String, String>();
        rtn.put("jobId", jobId);
        rtn.put("taskId", taskId);

        TraversePath tp = new TraversePath();
        TraverseBehavior tbcg = new TraverseBehavior(TraverseBehavior.TRAVERSE_MODE.FLATTEN, new TaskCounterGroupParser());
        tp.addPath("jobTaskAttemptCounters.taskAttemptCounterGroup", tbcg);

        rtn = recordConverter.convert(rtn, counterJson, "jobTaskAttemptCounters", tp);

        return rtn;
    }

    /*
    protected StringBuilder buildRecord(JsonNode node, String key, StringBuilder inSb, TRAVERSE traverse) {
        StringBuilder sb = null;
        if (inSb != null) {
            sb = new StringBuilder(inSb);
        } else {
            sb = new StringBuilder();
        }

        JsonNode startNode = null;
        if (key != null) {
            startNode = node.get(key);
        }

        sb = buildInnerRecord(startNode, sb, traverse);

        return sb;
    }

    protected StringBuilder buildInnerRecord(JsonNode node, StringBuilder sb, TRAVERSE traverse) {
//        StringBuilder sb = new StringBuilder(inSb);

        if (node.isValueNode()) {
            if (!header) {
                sb.append(delimiter).append(node.asText());
            }
        } else if (node.isContainerNode()) {
            Iterator<Map.Entry<String, JsonNode>> iter = node.getFields();

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> val = iter.next();
                if (header) {
                    sb.append(delimiter).append(val.getKey());
                } else {
                    buildInnerRecord(val.getValue(), sb);
                }
            }

        } else if (node.isArray()) {
            switch (traverse) {
                case FLATTEN:

                    break;
                case EXPLODE:

                    break;
            }

            // TODO: Need to handle Arrays. Requires new record...  But can't be done in line, it will corrupt current structure.
//            Iterator<JsonNode> aIter = node.iterator();
//            while (aIter.hasNext()) {
//                StringBuilder innerSb = new StringBuilder(sb);
//                innerSb = in
//            }
        }

        return sb;
    }
    */

}
