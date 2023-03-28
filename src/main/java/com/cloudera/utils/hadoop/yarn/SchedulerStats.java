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

import com.cloudera.utils.hadoop.yarn.parsers.QueueParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the queue stats .
 */
public class SchedulerStats extends ResourceManagerStats {

    public static final String URL_PATH = "/ws/v1/cluster/scheduler";

    public static final String QUEUE = "queue";
    public static final String QUEUE_USAGE = "queue_usage";

    // Used the actual output.  Docs aren't accurate.
    static final String[] QUEUE_FIELDS = {"reporting_ts", "queue.path",
            // shcedulerinfo
            "type",
            // parent queue
            "capacity", "usedCapacity", "maxCapacity", "absoluteCapacity", "absoluteMaxCapacity", "absoluteUsedCapacity",
            "numApplications", "usedResouces", "queueName", "state",
            // parent queue.resoucesUsed
            "resourcesUsed.memory", "resourcesUsed.vCores",
            // leaf queue
            "hideReservationQueues", "allocatedContainers", "reservedContainers",
            "pendingContainers",
            // capacities array

            // resources array

            // minEffectiveCapacity...
            "minEffectiveCapacity.memory", "minEffectiveCapacity.vCores",
            // maxEffectiveCapacity...
            "maxEffectiveCapacity.memory", "maxEffectiveCapacity.vCores",
            // maximumAllcation...
            "maximumAllocation.memory", "maximumAllocation.vCores",
            // queueAcls..

            // leaf queue cont.
            "queuePriority", "orderingPolicyInfo", "autoCreateChildQueueEnabled",
            // leafQueueTemplate
            "numActiveApplications", "numPendingApplications", "numContainers", "maxApplications",
            "maxApplicationsPerUser", "userLimit", "userLimitFactor", "configuredMaxAMResourceLimit",
            // AMResourceLimit..
            "AMResourceLimit.memory", "AMResourceLimit.vCores",
            // usedAMResource
            "usedAMResource.memory", "usedAMResource.vCores",
            // userAMResourceLimit
            "userAMResourceLimit.memory", "userAMResourceLimit.vCores",
            // leaf cont.
            "preemptionDisabled", "intraQueuePreemptionDisabled", "defaultPriority", "isAutoCreatedLeafQueue", "maxApplicationLifetime",
            "defaultApplicationLifetime"};

    static final String[] QUEUE_USAGE_FIELDS = {
            // parent
            "reporting_ts", "queue.path", "capacity", "usedCapacity", "maxCapacity",
            "absoluteCapacity", "absoluteMaxCapacity", "absoluteUsedCapacity", "numApplications", "queueName", "state",
            "hideReservationQueues", "allocatedContainers", "reservedContainers", "pendingContainers", "queuePriority",
            "orderingPolicyInfo", "autoCreateChildQueueEnabled", "numActiveApplications", "numPendingApplications", "numContainers",
            "maxApplications", "maxApplicationsPerUser", "userLimit",
            // users in queue.
            "user.numActiveApplications", "user.numPendingApplications",
            "user.username", "user.userWeight", "user.isActive", "user.resourcesUsed.memory", "user.resourcesUsed.vCores"};

    private static Map<String, String[]> recordFieldMap;

    static {
        recordFieldMap = new HashMap<String, String[]>();
        recordFieldMap.put(QUEUE, QUEUE_FIELDS);
        recordFieldMap.put(QUEUE_USAGE, QUEUE_USAGE_FIELDS);
    }

    public Map<String, String[]> getRecordFieldMap() {
        return recordFieldMap;
    }

    private String timestamp = null;

    public SchedulerStats(Configuration configuration) {
        super(configuration);
    }

    public SchedulerStats() {
    }

    //    @Override
    public String getDescription() {
        return "Collect Queue Stats from the YARN REST API";
    }

    @Override
    public void execute() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        this.timestamp = df.format(new Date());

        String baseRMUrlStr = getResourceManagerWebAddress();
        // Test with Call.
//        if (ResourceManagerResolvable(baseRMUrlStr)) {
//            System.out.println("Checking Alternate RM Location. " + baseRMUrlStr + " is the standby and can't process REST API Calls");
//            baseRMUrlStr = getResourceManagerWebAddress(true);
//        }

        System.out.println("Resource Manager Server URL: " + baseRMUrlStr);

        String rootPath = baseRMUrlStr + URL_PATH;

        try {

            URL schUrl = new URL(rootPath);

            URLConnection schConnection = schUrl.openConnection();
            String schJson = IOUtils.toString(schConnection.getInputStream(), StandardCharsets.UTF_8);

            if (raw) {
//                print(QUEUE + "_raw", schJson);
            } else {

                QueueParser queueParser = new QueueParser(schJson);
                Map<String, List<Map<String, Object>>> queueSet = queueParser.getQueues(timestamp);

                Iterator<Map.Entry<String, List<Map<String, Object>>>> qIter = queueSet.entrySet().iterator();

                while (qIter.hasNext()) {
                    Map.Entry<String, List<Map<String, Object>>> entry = qIter.next();
                    addRecords(entry.getKey(), entry.getValue());
                }

                /*
                1. Get current queue state.

                 */


//                Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getRecords().entrySet().iterator();
//                while (rIter.hasNext()) {
//                    Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
//                    System.out.println("Key: " + recordSet.getKey());
//                    print(recordSet.getKey(), recordFieldMap.get(recordSet.getKey()), recordSet.getValue());
//                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    //    @Override
    public void process(CommandLine cmdln) {
        init(cmdln);
        execute();
    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Queue Stats from the YARN REST API.").append("\n");


        System.out.println(sb.toString());
    }


}
