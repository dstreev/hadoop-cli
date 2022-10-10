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

package com.cloudera.utils.hadoop.hdfs.util;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by streever on 2016-03-21.
 */
public class NamenodeJmxParser {

    public static String NN_STATUS_JMX_BEAN = "Hadoop:service=NameNode,name=NameNodeStatus";

    private static String TOP_USER_OPS_COUNT = "TopUserOpCounts";

    private JmxJsonParser jjp = null;
    private String delimiter = "\u0001";

    private Map<String, Object> metadata = new LinkedHashMap<String, Object>();


    public NamenodeJmxParser(String resource) throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream(resource);
//        InputStream dataIn = classLoader.getResourceAsStream(resource);
        String resourceJson = IOUtils.toString(resourceStream);

        init(resourceJson, resourceJson);
    }

    public NamenodeJmxParser(String statusIn, String dataIn) throws Exception {
        init(statusIn, dataIn);
    }

    private void init(String statusIn, String dataIn) throws Exception {
        JmxJsonParser statusjp = new JmxJsonParser(statusIn);
        jjp = new JmxJsonParser(dataIn);

        // Get Host Info
        Map<String, Object> nnStatusMap = statusjp.getJmxBeanContent(NN_STATUS_JMX_BEAN);
        String[] statusInclude = {"HostAndPort", "State"};

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        metadata.put("Timestamp", df.format(new Date()));

        transferTo(metadata, nnStatusMap, statusInclude);

    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    private void transferTo(Map<String, Object> target, Map<String, Object> source, String[] transferItems) {
        if (transferItems == null) {
            for (Map.Entry<String, Object> entry : source.entrySet()) {
                target.put(entry.getKey(), entry.getValue());
            }
        } else {
            for (String transferItem : transferItems) {
                if (source.containsKey(transferItem)) {
                    target.put(transferItem, source.get(transferItem));
                } else {
                    System.out.println("Source doesn't contain key: " + transferItem);
                }
            }
        }
    }

    private Map<String,Object> mapToReducedMap(Map<String, Object> map, String[] fields) {
        Map<String,Object> rtn = new LinkedHashMap<>();
        if (fields == null) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                rtn.put(entry.getKey(), entry.getValue());
            }
        } else {
            for (String field: fields) {
                if (map.containsKey(field)) {
                    rtn.put(field, map.get(field));
//                    rtn.add(map.get(field).toString());
                } else {
                    System.out.println("Map doesn't contain key: " + field);
                }
            }
        }
        return rtn;
    }

    public String listToString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iItems = list.iterator();
        while (iItems.hasNext()) {
            sb.append(iItems.next());
            if (iItems.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

    public List<Map<String,Object>> getTopUserOpRecords() {
        List<Map<String,Object>> rtn = null;
        Map<String, Object> fsState = jjp.getJmxBeanContent(NamenodeJmxBean.FS_STATE_JMX_BEAN.getBeanName());

        ObjectMapper mapper = new ObjectMapper();
        String tuocStr = fsState.get(TOP_USER_OPS_COUNT).toString();

        try {
            JsonNode tuocNode = mapper.readValue(tuocStr.getBytes(), JsonNode.class);
            rtn = buildTopUserOpsCountRecords(tuocNode);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rtn;
    }

    public Map<String, Object> getNamenodeInfo() {

        Map<String,Object> working = new LinkedHashMap<>(metadata);

        Map<String, Object> nnInfo = jjp.getJmxBeanContent(NamenodeJmxBean.NN_INFO_JMX_BEAN.getBeanName());

        String[] fields = {"Version", "Used", "Free", "Safemode", "TotalBlocks", "TotalFiles", "NumberOfMissingBlocks"};
    // Doesn't exist in 2.2.x
//                , "NumberOfMissingBlocksWithReplicationFactorOne"};

        Map<String,Object> fieldMap = mapToReducedMap(nnInfo, fields);

        working.putAll(fieldMap);

        return working;

        /*
            "name" : "Hadoop:service=NameNode,name=NameNodeInfo",
    "modelerType" : "org.apache.hadoop.hdfs.server.namenode.FSNamesystem",
    "Total" : 1254254346240,
    "UpgradeFinalized" : true,
    "ClusterId" : "CID-b255ee79-e4f1-44a8-b134-044c25d7bfd4",
    "Version" : "2.7.1.2.3.5.1-68, rfe3c6b6dd1526d3c46f61a2e8fab9bb5eb649989",
    "Used" : 43518906368,
    "Free" : 1203736371256,
    "Safemode" : "",
    "NonDfsUsedSpace" : 6999068616,
    "PercentUsed" : 3.4697034,
    "BlockPoolUsedSpace" : 43518906368,
    "PercentBlockPoolUsed" : 3.4697034,
    "PercentRemaining" : 95.972275,
    "CacheCapacity" : 0,
    "CacheUsed" : 0,
    "TotalBlocks" : 7813,
    "TotalFiles" : 9555,
    "NumberOfMissingBlocks" : 0,
    "NumberOfMissingBlocksWithReplicationFactorOne" : 0,

         */
    }

    public Map<String, Object> getFSState() {

        Map<String, Object> working = new LinkedHashMap<>(metadata);

        Map<String, Object> fsState = jjp.getJmxBeanContent(NamenodeJmxBean.FS_STATE_JMX_BEAN.getBeanName());

        String[] fields = {"CapacityUsed", "CapacityRemaining", "BlocksTotal", "PendingReplicationBlocks", "UnderReplicatedBlocks", "ScheduledReplicationBlocks", "PendingDeletionBlocks", "FSState", "NumLiveDataNodes", "NumDeadDataNodes", "NumDecomLiveDataNodes", "NumDecomDeadDataNodes", "VolumeFailuresTotal"};


        Map<String, Object> fieldMap = mapToReducedMap(fsState, fields);

        working.putAll(fieldMap);

        return working;

        /*
            "CapacityTotal" : 1254254346240,
    "CapacityUsed" : 43518906368,
    "CapacityRemaining" : 1203736371256,
    "TotalLoad" : 36,
    "SnapshotStats" : "{\"SnapshottableDirectories\":4,\"Snapshots\":8}",
    "FsLockQueueLength" : 0,
    "BlocksTotal" : 7813,
    "MaxObjects" : 0,
    "FilesTotal" : 9555,
    "PendingReplicationBlocks" : 0,
    "UnderReplicatedBlocks" : 4,
    "ScheduledReplicationBlocks" : 0,
    "PendingDeletionBlocks" : 0,
    "BlockDeletionStartTime" : 1458341216736,
    "FSState" : "Operational",
    "NumLiveDataNodes" : 3,
    "NumDeadDataNodes" : 0,
    "NumDecomLiveDataNodes" : 0,
    "NumDecomDeadDataNodes" : 0,
    "VolumeFailuresTotal" : 0,
    "EstimatedCapacityLostTotal" : 0,
    "NumDecommissioningDataNodes" : 0,
    "NumStaleDataNodes" : 0,
    "NumStaleStorages" : 0,

         */
    }

    private List<Map<String,Object>> buildTopUserOpsCountRecords(JsonNode topNode) {
        List<Map<String,Object>> rtn = null;
        if (topNode != null) {
            rtn = new ArrayList<Map<String,Object>>();
            try {
//                StringBuilder sbHeader = new StringBuilder();
                // Build the Key for the Record.
//                for (String key : metadata.keySet()) {
//                    sbHeader.append(metadata.get(key)).append(delimiter);
//                }

                // Cycle through the Windows
                for (JsonNode wNode : topNode.get("windows")) {
                    Map<String, Object> winMap = new LinkedHashMap<String, Object>(metadata);
//                    StringBuilder sbWindow = new StringBuilder(sbHeader);
                    winMap.put("windowLenMs", wNode.get("windowLenMs").asText());
//                    sbWindow.append(wNode.get("windowLenMs").asText()).append(delimiter);
                    // Cycle through the Operations.
                    for (JsonNode opNode : wNode.get("ops")) {
                        // Get Op Type
//                        StringBuilder sbOp = new StringBuilder(sbWindow);
                        Map<String, Object> opMap = new LinkedHashMap<String, Object>(winMap);
//                        sbOp.append(opNode.get("opType").asText()).append(delimiter);
                        opMap.put("opType", opNode.get("opType").asText());
                        // Cycle through the Users.
                        for (JsonNode userNode : opNode.get("topUsers")) {
                            Map<String, Object> userMap = new LinkedHashMap<String, Object>(opMap);
//                            StringBuilder sbUser = new StringBuilder(sbOp);
//                            sbUser.append(userNode.get("user").asText()).append(delimiter);
//                            sbUser.append(userNode.get("count").asText());
                            userMap.put("user", userNode.get("user").asText());
                            userMap.put("count", userNode.get("count").asText());
                            // Add to the list.
                            rtn.add(userMap);
                        }
                    }
                }

            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        return rtn;
    }
}
