package com.dstreev.hadoop.hdfs.util;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dstreev on 2016-03-21.
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

    private List<String> mapValuesToList(Map<String, Object> map, String[] fields) {
        List<String> rtn = new LinkedList<>();
        if (fields == null) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                rtn.add(entry.getValue().toString());
            }
        } else {
            for (String field: fields) {
                if (map.containsKey(field)) {
                    rtn.add(map.get(field).toString());
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

    public List<String> getTopUserOpRecords() {
        List<String> rtn = null;
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

    public String getNamenodeInfo() {

        List<String> working = mapValuesToList(metadata, null);

        Map<String, Object> nnInfo = jjp.getJmxBeanContent(NamenodeJmxBean.NN_INFO_JMX_BEAN.getBeanName());

        String[] fields = {"Version", "Used", "Free", "Safemode", "TotalBlocks", "TotalFiles", "NumberOfMissingBlocks", "NumberOfMissingBlocksWithReplicationFactorOne"};

        List<String> fieldList = mapValuesToList(nnInfo, fields);

        working.addAll(fieldList);

        return listToString(working);
//        return working;

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

    public String getFSState() {

        List<String> working = mapValuesToList(metadata, null);

        Map<String, Object> fsState = jjp.getJmxBeanContent(NamenodeJmxBean.FS_STATE_JMX_BEAN.getBeanName());

        String[] fields = {"CapacityUsed", "CapacityRemaining", "BlocksTotal", "PendingReplicationBlocks", "UnderReplicatedBlocks", "ScheduledReplicationBlocks", "PendingDeletionBlocks", "FSState", "NumLiveDataNodes", "NumDeadDataNodes", "NumDecomLiveDataNodes", "NumDecomDeadDataNodes", "VolumeFailuresTotal"};


        List<String> fieldList = mapValuesToList(fsState, fields);

        working.addAll(fieldList);

        return listToString(working);

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

    private List<String> buildTopUserOpsCountRecords(JsonNode topNode) {
        List<String> rtn = null;
        if (topNode != null) {
            rtn = new ArrayList<String>();
            try {
                StringBuilder sbHeader = new StringBuilder();
                // Build the Key for the Record.
                for (String key : metadata.keySet()) {
                    sbHeader.append(metadata.get(key)).append(delimiter);
                }

                // Cycle through the Windows
                for (JsonNode wNode : topNode.get("windows")) {
                    StringBuilder sbWindow = new StringBuilder(sbHeader);

                    sbWindow.append(wNode.get("windowLenMs").asText()).append(delimiter);
                    // Cycle through the Operations.
                    for (JsonNode opNode : wNode.get("ops")) {
                        // Get Op Type
                        StringBuilder sbOp = new StringBuilder(sbWindow);
                        sbOp.append(opNode.get("opType").asText()).append(delimiter);
                        // Cycle through the Users.
                        for (JsonNode userNode : opNode.get("topUsers")) {
                            StringBuilder sbUser = new StringBuilder(sbOp);
                            sbUser.append(userNode.get("user").asText()).append(delimiter);
                            sbUser.append(userNode.get("count").asText());
                            // Add to the list.
                            rtn.add(sbUser.toString());
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
