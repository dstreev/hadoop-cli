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

package com.cloudera.utils.hadoop.yarn.parsers;

import com.cloudera.utils.hadoop.yarn.SchedulerStats;
import com.cloudera.utils.hadoop.util.RecordConverter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by streever on 2016-04-26.
 */
public class QueueParser {

    private List<String> skipAttrList = new LinkedList<String>();


    private ObjectMapper mapper = null;
    private JsonNode rootNode = null;

    public QueueParser(String json) throws IOException {
        mapper = new ObjectMapper();
        rootNode = mapper.readValue(json, JsonNode.class);
        skipAttrList.add("type");
    }

    public Map<String, List<Map<String, Object>>> getQueues(String timestamp) {


        JsonNode schedulerNode = rootNode.get("scheduler");
        JsonNode schedulerInfoNode = schedulerNode.get("schedulerInfo");
        JsonNode queuesNode = schedulerInfoNode.get("queues");

        List<String> queuePath = new LinkedList<String>();
        queuePath.add("root");
        List<Map<String, Object>> queueList = processQueues(timestamp, queuePath, queuesNode);

        List<String> queuePath2 = new LinkedList<String>();
        queuePath2.add("root");
        List<Map<String, Object>> usageList = processUsage(timestamp, queuePath2, queuesNode);

        // Now look for Queue Path and Queues and split them.
        Map<String, List<Map<String, Object>>> rtn = new LinkedHashMap<>();

        for (Map<String, Object> item : queueList) {
            if (item.containsKey("maxApplications")) {
                List<Map<String, Object>> qrList = rtn.get(SchedulerStats.QUEUE);
                if (qrList == null) {
                    qrList = new LinkedList<Map<String, Object>>();
                    rtn.put(SchedulerStats.QUEUE, qrList);
                }
                qrList.add(item);
            } else {
                List<Map<String, Object>> qpList = rtn.get(SchedulerStats.QUEUE_USAGE);
                if (qpList == null) {
                    qpList = new LinkedList<Map<String, Object>>();
                    rtn.put(SchedulerStats.QUEUE_USAGE, qpList);
                }
                qpList.add(item);
            }
        }

        rtn.put("queue_usage", usageList);

        return rtn;
    }

    protected List<Map<String, Object>> processQueues(String timestamp, List<String> queuePath, JsonNode queuesNode) {
        List<Map<String, Object>> rtn = new LinkedList<Map<String, Object>>();

        JsonNode queueArray = queuesNode.get("queue");
        if (queueArray.isArray()) {
            Iterator<JsonNode> qAryIter = queueArray.getElements();
            while (qAryIter.hasNext()) {
                JsonNode queueNode = qAryIter.next();
                String queueName = queueNode.get("queueName").asText();
                Map<String, Object> qRec = new LinkedHashMap<String, Object>();
                qRec.put("reporting_ts", timestamp);
                qRec.put("queue.path", buildPath(queuePath));

                qRec = buildQueueRecord(qRec, new LinkedList<String>(), queueNode);
                rtn.add(qRec);

                if (queueNode.has("queues")) {
                    List<String> iPath = new LinkedList<String>(queuePath);
                    iPath.add(queueName);
                    JsonNode iQueuesNode = queueNode.get("queues");
                    List<Map<String, Object>> iRtn = processQueues(timestamp, iPath, iQueuesNode);
                    rtn.addAll(iRtn);
                }

            }
        } else {
            // Wasn't expecting this.
        }

        return rtn;
    }

    protected List<Map<String, Object>> processUsage(String timestamp, List<String> queuePath, JsonNode queuesNode) {
        List<Map<String, Object>> rtn = new LinkedList<Map<String, Object>>();

        JsonNode queueArray = queuesNode.get("queue");
        if (queueArray.isArray()) {
            Iterator<JsonNode> qAryIter = queueArray.getElements();
            while (qAryIter.hasNext()) {
                JsonNode queueNode = qAryIter.next();
                String queueName = queueNode.get("queueName").asText();
                Map<String, Object> qRec = new LinkedHashMap<String, Object>();
                qRec.put("reporting_ts", timestamp);
                qRec.put("queue.path", buildPath(queuePath));

                if (queueNode.has("users") && !queueNode.get("users").isNull()) {
                    List<Map<String, Object>> recs = buildUsageRecords(qRec, new LinkedList<String>(), queueNode);
                    if (recs.size() > 0)
                        rtn.addAll(recs);
                }

                if (queueNode.has("queues")) {
                    List<String> iPath = new LinkedList<String>(queuePath);
                    iPath.add(queueName);
                    JsonNode iQueuesNode = queueNode.get("queues");
                    List<Map<String, Object>> iRtn = processUsage(timestamp, iPath, iQueuesNode);
                    if (iRtn.size() > 0)
                        rtn.addAll(iRtn);
                }


            }
        } else {
            // Wasn't expecting this.
        }

        return rtn;
    }

    protected List<Map<String, Object>> buildUsageRecords(Map<String, Object> record, List<String> treePath, JsonNode node) {

        List<Map<String, Object>> rtn = new LinkedList<Map<String, Object>>();

        if (node.isContainerNode()) {

            Iterator<Map.Entry<String, JsonNode>> iter = node.getFields();
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> val = iter.next();

                if (val.getValue().isArray()) {
                    if (val.getKey().equals("user")) {
                        Iterator<JsonNode> userNodeIter = val.getValue().getElements();
                        while (userNodeIter.hasNext()) {
                            JsonNode userNode = userNodeIter.next();
                            Map<String, Object> iRecord = new LinkedHashMap<String, Object>(record);
                            if (userNode.get("numActiveApplications") != null) {
                                iRecord.put("user.numActiveApplications", userNode.get("numActiveApplications").asText());
                            }
                            if (userNode.get("numPendingApplications") != null) {
                                iRecord.put("user.numPendingApplications", userNode.get("numPendingApplications").asText());
                            }
                            if (userNode.get("username") != null) {
                                iRecord.put("user.username", userNode.get("username").asText());
                            }
                            if (userNode.get("userWeight") != null) {
                                iRecord.put("user.userWeight", userNode.get("userWeight").asText());
                            }
                            if (userNode.get("isActive") != null) {
                                iRecord.put("user.isActive", userNode.get("isActive").asText());
                            }
                            JsonNode resNode = userNode.get("resourcesUsed");
                            if (resNode != null) {
                                if (resNode.get("memory") != null) {
                                    iRecord.put("user.resourcesUsed.memory", resNode.get("memory").asText());
                                }
                                if (resNode.get("vCores") != null){
                                    iRecord.put("user.resourcesUsed.vCores", resNode.get("vCores").asText());
                                }
                            }
                            rtn.add(iRecord);
                        }
                    }
                } else if (val.getValue().isContainerNode()) {
                    // Skip users.
                    if (!val.getKey().equals("users") && !val.getKey().equals("queues")) {
                        List<String> lclTp = new LinkedList<String>(treePath);
                        lclTp.add(val.getKey());
//                        rtn = buildQueueRecord(rtn, lclTp, val.getValue());
                    } else if (val.getKey().equals("users")) {
                        List<Map<String, Object>> iRtn = buildUsageRecords(record, treePath, val.getValue());
                        rtn.addAll(iRtn);
                    }
                } else {
                    if (!val.getKey().equals("users")) {

                        if (treePath.size() == 0) {
                            if (!skipAttrList.contains(val.getKey())) {
                                record.put(val.getKey(), val.getValue().asText());
                            }
                        } else {
                            if (!skipAttrList.contains(val.getKey())) {
                                List<String> lclTp = new LinkedList<String>(treePath);
                                lclTp.add(val.getKey());
                                record.put(buildPath(lclTp), val.getValue().asText());
                            }
                        }
                    }
                }

            }
        } else {
            // Wasn't expecting this.
        }

        return rtn;
    }

    protected Map<String, Object> buildQueueRecord(Map<String, Object> record, List<String> treePath, JsonNode node) {

        Map<String, Object> rtn = record;

        if (node.isContainerNode()) {
            Iterator<Map.Entry<String, JsonNode>> iter = node.getFields();
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> val = iter.next();
                if (val.getValue().isArray()) {
                    // Not expecting anything here.

                } else if (val.getValue().isContainerNode()) {
                    // Skip users.
                    if (!val.getKey().equals("users") && !val.getKey().equals("queues")) {
                        List<String> lclTp = new LinkedList<String>(treePath);
                        lclTp.add(val.getKey());
                        rtn = buildQueueRecord(rtn, lclTp, val.getValue());
                    }
                } else {
                    if (!val.getKey().equals("users")) {
                        if (treePath.size() == 0) {
                            try {
                                String strValue = RecordConverter.decode(val.getValue().asText());
//                                String strValue = URLDecoder.decode(val.getValue().asText(), StandardCharsets.UTF_8.toString());
//                                String strValue =
                                rtn.put(val.getKey(), strValue);
                            } catch (Throwable t) {
                                System.err.println("(Queue) Issue with decode: " + val.getKey() + ":" + val.getValue().asText());
                            }
                        } else {
                            List<String> lclTp = new LinkedList<String>(treePath);
                            lclTp.add(val.getKey());
                            rtn.put(buildPath(lclTp), val.getValue().asText());
                        }
                    }
                }
            }
        } else {
            // Wasn't expecting this.
        }

        return rtn;
    }

    protected String buildPath(List<String> path) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = path.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next());
            if (iter.hasNext())
                sb.append(".");
        }
        return sb.toString();

    }

}
