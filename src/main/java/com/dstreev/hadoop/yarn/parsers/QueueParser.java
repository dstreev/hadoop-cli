package com.dstreev.hadoop.yarn.parsers;

import com.dstreev.hadoop.util.RecordConverter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by dstreev on 2016-04-26.
 */
public class QueueParser {

    private ObjectMapper mapper = null;
    private JsonNode rootNode = null;

    public QueueParser(String json) throws IOException {
        mapper = new ObjectMapper();
        rootNode = mapper.readValue(json, JsonNode.class);
    }

    public Map<String, List<Map<String, String>>> getQueues() {

        List<String> queuePath = new LinkedList<String>();

        JsonNode schedulerNode = rootNode.get("scheduler");
        JsonNode schedulerInfoNode = schedulerNode.get("schedulerInfo");
        JsonNode queuesNode = schedulerInfoNode.get("queues");

        queuePath.add("root");
        List<Map<String, String>> queueList = processQueues(queuePath, queuesNode);

        // Now look for Queue Path and Queues and split them.
        Map<String, List<Map<String,String>>> rtn = new LinkedHashMap<>();

        for (Map<String, String> item: queueList) {
            if (item.containsKey("maxActiveApplications")) {
                List<Map<String,String>> qrList = rtn.get("queues");
                if (qrList == null) {
                    qrList = new LinkedList<Map<String,String>>();
                    rtn.put("queues",qrList);
                }
                qrList.add(item);
            } else {
                List<Map<String,String>> qpList = rtn.get("queuePaths");
                if (qpList == null) {
                    qpList = new LinkedList<Map<String,String>>();
                    rtn.put("queuePaths",qpList);
                }
                qpList.add(item);
            }
        }

        return rtn;
    }

    protected List<Map<String, String>> processQueues(List<String> queuePath, JsonNode queuesNode) {
        List<Map<String, String>> rtn = new LinkedList<Map<String, String>>();

        JsonNode queueArray = queuesNode.get("queue");
        if (queueArray.isArray()) {
            Iterator<JsonNode> qAryIter = queueArray.getElements();
            while (qAryIter.hasNext()) {
                JsonNode queueNode = qAryIter.next();
                String queueName = queueNode.get("queueName").asText();
                Map<String, String> qRec = new LinkedHashMap<String, String>();
                qRec.put("queue.path", buildPath(queuePath));
                qRec = buildQueueRecord(qRec, new LinkedList<String>(), queueNode);
                rtn.add(qRec);

                if (queueNode.has("queues")) {
                    List<String> iPath = new LinkedList<String>(queuePath);
                    iPath.add(queueName);
                    JsonNode iQueuesNode = queueNode.get("queues");
                    List<Map<String, String>> iRtn = processQueues(iPath, iQueuesNode);
                    rtn.addAll(iRtn);
                }

            }
        } else {
            // Wasn't expecting this.
        }

        return rtn;
    }


    protected Map<String, String> buildQueueRecord(Map<String, String> record, List<String> treePath, JsonNode node) {

        Map<String, String> rtn = record;

        if (node.isContainerNode()) {
            Iterator<Map.Entry<String, JsonNode>> iter = node.getFields();
            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> val = iter.next();
                if (val.getValue().isArray()) {

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
                            rtn.put(val.getKey(), val.getValue().asText());
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
