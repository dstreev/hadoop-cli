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

package com.cloudera.utils.hadoop.yarn;

import com.cloudera.utils.hadoop.AbstractQueryTimeFrameStats;
import com.cloudera.utils.hadoop.hdfs.shell.command.Direction;
import com.cloudera.utils.hadoop.shell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the stats on applications since the last time this was run or up to
 * 'n' (limit).
 */
public class ContainerStats extends AbstractQueryTimeFrameStats {
    public static final String APP = "app";
    // Not helpful for workload analysis.  Leaving out for now.
    public static final String ATTEMPT = "attempt";
    // Very fine grained.  Will leave out, for now.
    public static final String ATTEMPT_CONTAINERS = "attemptContainers";

    static final String[] APP_FIELDS = {"reporting_ts", "id", "user", "name", "queue", "state", "finalStatus", "progress", "trackingUI",
            "trackingUrl", "diagnostics", "clusterId", "applicationType", "applicationTags", "priority", "startedTime",
            "launchTime", "finishedTime", "elapsedTime", "amContainerLogs", "amHostHttpAddress", "amRPCAddress",
            "masterNodeId","allocatedMB", "allocatedVCores",
            "reservedMB", "reservedVCores",
            "runningContainers", "memorySeconds", "vcoreSeconds", "queueUsagePercentage",
            "clusterUsagePercentage", "preemptedResourceMB", "preemptedResourceVCores",
            "numNonAMContainerPreempted",
            "numAMContainerPreempted", "logAggregationStatus", "unmanagedApplication", "appNodeLabelExpression",
            "amNodeLabelExpression"};

    // Not using currently.  Wasn't sure this added much value for workload analysis.
    static final String[] APP_ATTEMPT_FIELDS = {"id", "nodeId", "nodeHttpAddress", "logsLink", "containerId", "startTime"};

    private static Map<String, String[]> recordFieldMap;

    static {
        recordFieldMap = new HashMap<String, String[]>();
        recordFieldMap.put(APP, APP_FIELDS);
        recordFieldMap.put(ATTEMPT, APP_ATTEMPT_FIELDS);
    }

    public ContainerStats(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "Collect Container Stats from the YARN REST API";
    }

    public ContainerStats(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public ContainerStats(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public ContainerStats(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public ContainerStats(String name, Environment env) {
        super(name, env);
    }

    @Override
    public void process(CommandLine cmdln) {

        String baseRMUrlStr = getResourceManagerWebAddress();
        // Test with Call.
//        if (ResourceManagerResolvable(baseRMUrlStr)) {
//            System.out.println("Checking Alternate RM Location. " + baseRMUrlStr + " is the standby and can't process REST API Calls");
//            baseRMUrlStr = getResourceManagerWebAddress(true);
//        }

        System.out.println("Resource Manager Server URL: " + baseRMUrlStr);

        String rootPath = baseRMUrlStr + "/ws/v1/cluster/apps";

        Map<String, String> queries = getQueries(cmdln);

        Iterator<Map.Entry<String, String>> iQ = queries.entrySet().iterator();

        while (iQ.hasNext()) {
            Map.Entry<String, String> entry = iQ.next();
            System.out.println("Resource Manager Query Parameters: " + entry.getValue());

            String query = entry.getKey();

            try {
                URL appsUrl = new URL(rootPath + "?" + query);

                URLConnection appsConnection = appsUrl.openConnection();
                String appsJson = IOUtils.toString(appsConnection.getInputStream(), StandardCharsets.UTF_8);

                if (raw) {
                    print(APP + "_raw", appsJson);
                } else {
                    YarnAppRecordConverter yarnRc = new YarnAppRecordConverter();

                    // Get Apps List and generate full item list.

                    List<Map<String, Object>> apps = yarnRc.apps(appsJson);

                    for (Map<String, Object> appMap : apps) {

                        addRecord(APP, appMap);
                        // SKipping Attempts for now.
                        //                        String appId = appMap.get("id").toString();

                        // App Attempts
//                        URL tasksUrl = new URL(getProtocol() + rootPath + "/" + appId + "/appattempts");
//                        URLConnection tasksConnection = tasksUrl.openConnection();
//                        String tasksJson = IOUtils.toString(tasksConnection.getInputStream());

                        // Not sure this adds much
//                    List<Map<String, Object>> attemptsList = yarnRc.appAttempts(tasksJson, appId);
//
//                    for (Map<String, Object> attemptMap : attemptsList) {
//                        addRecord(ATTEMPT, attemptMap);
//                    }

                    }
                }
            } catch (MalformedURLException ure) {
                ure.printStackTrace();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

            if (!raw) {
                Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getRecords().entrySet().iterator();
                while (rIter.hasNext()) {
                    Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
                    print(recordSet.getKey(), recordFieldMap.get(recordSet.getKey()), recordSet.getValue());
                }
            }
            clearCache();
        }

    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Container Stats for the YARN REST API.").append("\n");


        System.out.println(sb.toString());
    }


}
