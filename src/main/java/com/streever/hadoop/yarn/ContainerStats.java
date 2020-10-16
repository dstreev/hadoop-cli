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

package com.streever.hadoop.yarn;

import com.streever.hadoop.AbstractStats;
import com.streever.hadoop.hdfs.shell.command.Direction;
import com.streever.hadoop.shell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
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
public class ContainerStats extends AbstractStats {
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
                String appsJson = IOUtils.toString(appsConnection.getInputStream());

                if (raw) {
                    System.out.println(appsJson);
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
        sb.append("Collect Job History Server Stats for the JMX url.").append("\n");


        System.out.println(sb.toString());
    }


}
