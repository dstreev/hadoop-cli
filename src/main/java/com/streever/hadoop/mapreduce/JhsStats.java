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

package com.streever.hadoop.mapreduce;

import com.streever.hadoop.AbstractQueryTimeFrameStats;
import com.streever.hadoop.AbstractStats;
import com.streever.hadoop.hdfs.shell.command.Direction;
import com.streever.hadoop.shell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Job History Server URL, collect the stats on jobs since the last time this was run or up to
 * 'n' (limit).
 */
public class JhsStats extends AbstractQueryTimeFrameStats {

    public JhsStats(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "Collect Job History Server Stats for the JMX url";
    }

    public JhsStats(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public JhsStats(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public JhsStats(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public JhsStats(String name, Environment env) {
        super(name, env);
    }


    @Override
    public void process(CommandLine cmd) {

        String hostAndPort = configuration.get("mapreduce.jobhistory.webapp.address");

        System.out.println("Job History Server URL: " + hostAndPort);

        String rootPath = hostAndPort + "/ws/v1/history/mapreduce/jobs";

        Map<String, String> queries = getQueries(cmd);

        Iterator<Map.Entry<String, String>> iQ = queries.entrySet().iterator();

        while (iQ.hasNext()) {

            Map.Entry<String, String> entry = iQ.next();

            String query = entry.getKey();
            System.out.println("Query: " + entry.getValue());

            try {
                URL jobs = new URL(getProtocol() + rootPath + "?" + query);

                URLConnection jobsConnection = jobs.openConnection();
                String jobsJson = IOUtils.toString(jobsConnection.getInputStream());

                MRJobRecordConverter mrc = new MRJobRecordConverter();

                // Get Jobs.
                List<String> jobIdList = mrc.jobIdList(jobsJson);

                for (String jobId : jobIdList) {
                    System.out.println(jobId);
                    // Get Job Detail   <api>/<job_id>
                    URL jobUrl = new URL(getProtocol() + rootPath + "/" + jobId);
                    URLConnection jobConnection = jobUrl.openConnection();
                    String jobJson = IOUtils.toString(jobConnection.getInputStream());

                    Map<String, Object> jobDetailMap = mrc.jobDetail(jobJson);
                    addRecord("job", jobDetailMap);

                    // Job Counter
                    URL jobCounter = new URL(getProtocol() + rootPath + "/" + jobId + "/counters");
                    URLConnection jobCounterConnection = jobCounter.openConnection();
                    String jobCounterJson = IOUtils.toString(jobCounterConnection.getInputStream());

                    Map<String, Object> jobCounterMap = mrc.jobCounters(jobCounterJson);
                    addRecord("jobCounter", jobCounterMap);

                    // Tasks
                    URL tasksUrl = new URL(getProtocol() + rootPath + "/" + jobId + "/tasks");
                    URLConnection tasksConnection = tasksUrl.openConnection();
                    String tasksJson = IOUtils.toString(tasksConnection.getInputStream());

                    List<String> taskIdList = mrc.jobTaskList(tasksJson);

                    for (String taskId : taskIdList) {

                        // Get Task Detail  <api>/<job_id>/tasks/<task_id> (can get all this from task list above)
                        URL taskUrl = new URL(getProtocol() + rootPath + "/" + jobId + "/tasks/" + taskId);
                        URLConnection taskConnection = taskUrl.openConnection();
                        String taskJson = IOUtils.toString(taskConnection.getInputStream());

                        Map<String, Object> taskDetailMap = mrc.taskDetail(jobId, taskJson);
                        addRecord("task", taskDetailMap);

                        // Get Task Counters <api>/<job_id>/task/<task_id>/counters
                        URL taskCounterUrl = new URL(getProtocol() + rootPath + "/" + jobId + "/tasks/" + taskId + "/counters");
                        URLConnection taskCounterConnection = taskCounterUrl.openConnection();
                        String taskCounterJson = IOUtils.toString(taskCounterConnection.getInputStream());

                        Map<String, Object> taskCounterMap = mrc.taskCounter(jobId, taskCounterJson);
                        addRecord("taskCounter", taskCounterMap);

                        // Task Attempts
                        URL taskAttemptsUrl = new URL(getProtocol() + rootPath + "/" + jobId + "/tasks/" + taskId + "/attempts");
                        URLConnection taskAttemptsConnection = taskAttemptsUrl.openConnection();
                        String taskAttemptsJson = IOUtils.toString(taskAttemptsConnection.getInputStream());

                        // Get Task Attempts List
                        List<String> taskAttemptIdList = mrc.taskAttemptList(taskAttemptsJson);

                        for (String taskAttemptId : taskAttemptIdList) {
                            // Task Attempt
                            URL taskAttemptUrl = new URL(getProtocol() + rootPath + "/" + jobId + "/tasks/" + taskId + "/attempts/" + taskAttemptId);
                            URLConnection taskAttemptConnection = taskAttemptUrl.openConnection();
                            String taskAttemptJson = IOUtils.toString(taskAttemptConnection.getInputStream());

                            Map<String, Object> taskAttemptMap = mrc.attemptDetail(jobId, taskId, taskAttemptJson);
                            addRecord("taskAttempt", taskAttemptMap);

                            // Task Attempt Counter
                            URL taskAttemptCountersUrl = new URL(getProtocol() + rootPath + "/" + jobId + "/tasks/" + taskId + "/attempts/" + taskAttemptId + "/counters");
                            URLConnection taskAttemptCountersConnection = taskAttemptCountersUrl.openConnection();
                            String taskAttemptCounterJson = IOUtils.toString(taskAttemptCountersConnection.getInputStream());

                            Map<String, Object> taskAttemptCounterMap = mrc.attemptCounter(jobId, taskId, taskAttemptCounterJson);
                            addRecord("taskAttemptCounter", taskAttemptCounterMap);

                        }
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
                // TODO: Fix
//                print(recordSet.getKey(), recordSet.getValue());
            }

            // Clear for next query
            clearCache();
        }
    }


    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Job History Server Stats for the JMX url.").append("\n");


        System.out.println(sb.toString());
    }

}
