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
import com.streever.tools.stemshell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
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

        String hostAndPort = configuration.get("yarn.resourcemanager.webapp.address");

        System.out.println("Resource Manager Server URL: " + hostAndPort);

        String rootPath = hostAndPort + "/ws/v1/cluster/apps";

        Map<String, String> queries = getQueries(cmdln);

        Iterator<Map.Entry<String, String>> iQ = queries.entrySet().iterator();

        while (iQ.hasNext()) {

            Map.Entry<String, String> entry = iQ.next();

            String query = entry.getKey();
            System.out.println("Query: " + entry.getValue());
            try {
                URL appsUrl = new URL("http://" + rootPath + "?" + query);

                URLConnection appsConnection = appsUrl.openConnection();
                String appsJson = IOUtils.toString(appsConnection.getInputStream());

                YarnAppRecordConverter yarnRc = new YarnAppRecordConverter();

                // Get Apps List of Ids and process singularly.
                /*
                List<String> appIdList = yarnRc.appIdList(jobsJson);

                for (String appId : appIdList) {
                    System.out.println(appId);

                    // Get App Detail   <api>/<job_id>
                    URL appUrl = new URL("http://" + rootPath + "/" + appId);
                    URLConnection appConnection = appUrl.openConnection();
                    String appJson = IOUtils.toString(appConnection.getInputStream());

                    Map<String, String> jobDetailMap = yarnRc.detail(appJson, "app");
                    addRecord("app", appMap);


                    // App Attempts
                    URL attemptsUrl = new URL("http://" + rootPath + "/" + appId + "/appattempts");
                    URLConnection attemptsConnection = attemptsUrl.openConnection();
                    String attemptsJson = IOUtils.toString(attemptsConnection.getInputStream());

                    List<Map<String,String>> attemptsList = yarnRc.appAttempts(attemptsJson, appId);

                    for (Map<String,String> attemptMap: attemptsList) {
                        addRecord("attempt", attemptMap);
                    }

                }
                */

                // Get Apps List and generate full item list.

                List<Map<String, Object>> apps = yarnRc.apps(appsJson);

                for (Map<String, Object> appMap : apps) {

                    addRecord("app", appMap);
                    String appId = appMap.get("id").toString();

                    // App Attempts
                    URL tasksUrl = new URL("http://" + rootPath + "/" + appId + "/appattempts");
                    URLConnection tasksConnection = tasksUrl.openConnection();
                    String tasksJson = IOUtils.toString(tasksConnection.getInputStream());

                    List<Map<String, Object>> attemptsList = yarnRc.appAttempts(tasksJson, appId);

                    for (Map<String, Object> attemptMap : attemptsList) {
                        addRecord("attempt", attemptMap);
                    }

                }
            } catch (MalformedURLException ure) {
                ure.printStackTrace();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

            Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getRecords().entrySet().iterator();
            while (rIter.hasNext()) {
                Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
                print(recordSet.getKey(), recordSet.getValue());
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
