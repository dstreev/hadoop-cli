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

package com.cloudera.utils.hadoop.mapreduce;

import com.cloudera.utils.hadoop.AbstractQueryTimeFrameStats;
import com.cloudera.utils.hadoop.hdfs.shell.command.Direction;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class JhsStats extends AbstractQueryTimeFrameStats {

    public JhsStats(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "Collect Job History Server Stats for the JMX url";
    }

    public JhsStats(String name, CliEnvironment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public JhsStats(String name, CliEnvironment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public JhsStats(String name, CliEnvironment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public JhsStats(String name, CliEnvironment env) {
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
                log.error("URL Exception: ", ure);
            } catch (IOException ioe) {
                log.error("IO Exception: ", ioe);
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
