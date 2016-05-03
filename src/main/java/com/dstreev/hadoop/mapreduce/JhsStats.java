package com.dstreev.hadoop.mapreduce;

import com.dstreev.hadoop.AbstractStats;
import com.dstreev.hadoop.hdfs.shell.command.Constants;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.dstreev.hadoop.util.HdfsWriter;
import com.dstreev.hadoop.util.RecordConverter;
import com.instanceone.hdfs.shell.command.HdfsAbstract;
import com.instanceone.stemshell.Environment;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dstreev on 2016-04-25.
 * <p>
 * Using the Job History Server URL, collect the stats on jobs since the last time this was run or up to
 * 'n' (limit).
 */
public class JhsStats extends AbstractStats {

    public JhsStats(String name) {
        super(name);
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
                URL jobs = new URL("http://" + rootPath + "?" + query);

                URLConnection jobsConnection = jobs.openConnection();
                String jobsJson = IOUtils.toString(jobsConnection.getInputStream());

                MRJobRecordConverter mrc = new MRJobRecordConverter();

                // Get Jobs.
                List<String> jobIdList = mrc.jobIdList(jobsJson);

                for (String jobId : jobIdList) {
                    System.out.println(jobId);
                    // Get Job Detail   <api>/<job_id>
                    URL jobUrl = new URL("http://" + rootPath + "/" + jobId);
                    URLConnection jobConnection = jobUrl.openConnection();
                    String jobJson = IOUtils.toString(jobConnection.getInputStream());

                    Map<String, Object> jobDetailMap = mrc.jobDetail(jobJson);
                    addRecord("job", jobDetailMap);

                    // Job Counter
                    URL jobCounter = new URL("http://" + rootPath + "/" + jobId + "/counters");
                    URLConnection jobCounterConnection = jobCounter.openConnection();
                    String jobCounterJson = IOUtils.toString(jobCounterConnection.getInputStream());

                    Map<String, Object> jobCounterMap = mrc.jobCounters(jobCounterJson);
                    addRecord("jobCounter", jobCounterMap);

                    // Tasks
                    URL tasksUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks");
                    URLConnection tasksConnection = tasksUrl.openConnection();
                    String tasksJson = IOUtils.toString(tasksConnection.getInputStream());

                    List<String> taskIdList = mrc.jobTaskList(tasksJson);

                    for (String taskId : taskIdList) {

                        // Get Task Detail  <api>/<job_id>/tasks/<task_id> (can get all this from task list above)
                        URL taskUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks/" + taskId);
                        URLConnection taskConnection = taskUrl.openConnection();
                        String taskJson = IOUtils.toString(taskConnection.getInputStream());

                        Map<String, Object> taskDetailMap = mrc.taskDetail(jobId, taskJson);
                        addRecord("task", taskDetailMap);

                        // Get Task Counters <api>/<job_id>/task/<task_id>/counters
                        URL taskCounterUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks/" + taskId + "/counters");
                        URLConnection taskCounterConnection = taskCounterUrl.openConnection();
                        String taskCounterJson = IOUtils.toString(taskCounterConnection.getInputStream());

                        Map<String, Object> taskCounterMap = mrc.taskCounter(jobId, taskCounterJson);
                        addRecord("taskCounter", taskCounterMap);

                        // Task Attempts
                        URL taskAttemptsUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks/" + taskId + "/attempts");
                        URLConnection taskAttemptsConnection = taskAttemptsUrl.openConnection();
                        String taskAttemptsJson = IOUtils.toString(taskAttemptsConnection.getInputStream());

                        // Get Task Attempts List
                        List<String> taskAttemptIdList = mrc.taskAttemptList(taskAttemptsJson);

                        for (String taskAttemptId : taskAttemptIdList) {
                            // Task Attempt
                            URL taskAttemptUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks/" + taskId + "/attempts/" + taskAttemptId);
                            URLConnection taskAttemptConnection = taskAttemptUrl.openConnection();
                            String taskAttemptJson = IOUtils.toString(taskAttemptConnection.getInputStream());

                            Map<String, Object> taskAttemptMap = mrc.attemptDetail(jobId, taskId, taskAttemptJson);
                            addRecord("taskAttempt", taskAttemptMap);

                            // Task Attempt Counter
                            URL taskAttemptCountersUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks/" + taskId + "/attempts/" + taskAttemptId + "/counters");
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
                print(recordSet.getKey(), recordSet.getValue());
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
