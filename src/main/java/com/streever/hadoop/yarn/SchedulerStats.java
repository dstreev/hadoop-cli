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
import com.streever.hadoop.yarn.parsers.QueueParser;
import com.streever.hadoop.shell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the queue stats .
 */
public class SchedulerStats extends AbstractStats {

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

    private String timestamp = null;

    public SchedulerStats(String name) {
        super(name);
    }

    public SchedulerStats(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public SchedulerStats(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public SchedulerStats(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public SchedulerStats(String name, Environment env) {
        super(name, env);
    }

    @Override
    public void process(CommandLine cmdln) {

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        this.timestamp = df.format(new Date());

        String baseRMUrlStr = getResourceManagerWebAddress();
        // Test with Call.
//        if (ResourceManagerResolvable(baseRMUrlStr)) {
//            System.out.println("Checking Alternate RM Location. " + baseRMUrlStr + " is the standby and can't process REST API Calls");
//            baseRMUrlStr = getResourceManagerWebAddress(true);
//        }

        System.out.println("Resource Manager Server URL: " + baseRMUrlStr);

        String rootPath = baseRMUrlStr + "/ws/v1/cluster/scheduler";

        try {

            URL schUrl = new URL(rootPath);

            URLConnection schConnection = schUrl.openConnection();
            String schJson = IOUtils.toString(schConnection.getInputStream());

            if (raw) {
                System.out.println(schJson);
                return;
            }

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


            Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getRecords().entrySet().iterator();
            while (rIter.hasNext()) {
                Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
                System.out.println("Key: " + recordSet.getKey());
                print(recordSet.getKey(), recordFieldMap.get(recordSet.getKey()), recordSet.getValue());
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Queue Stats from the YARN REST API.").append("\n");


        System.out.println(sb.toString());
    }


}
