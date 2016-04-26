package com.dstreev.hadoop.yarn;

import com.dstreev.hadoop.AbstractStats;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.instanceone.stemshell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by dstreev on 2016-04-25.
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

        List<String> queries = getQueries(cmdln);

        for (String query: queries) {
            try {
                System.out.println("Query: " + query);
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

                List<Map<String,String>> apps = yarnRc.apps(appsJson);

                for (Map<String, String> appMap: apps) {

                    addRecord("app", appMap);
                    String appId = appMap.get("id");

                    // App Attempts
                    URL tasksUrl = new URL("http://" + rootPath + "/" + appId + "/appattempts");
                    URLConnection tasksConnection = tasksUrl.openConnection();
                    String tasksJson = IOUtils.toString(tasksConnection.getInputStream());

                    List<Map<String,String>> attemptsList = yarnRc.appAttempts(tasksJson, appId);

                    for (Map<String,String> attemptMap: attemptsList) {
                        addRecord("attempt", attemptMap);
                    }

                }



                Iterator<Map.Entry<String, List<Map<String, String>>>> rIter = getRecords().entrySet().iterator();
                while (rIter.hasNext()) {
                    Map.Entry<String, List<Map<String, String>>> recordSet = rIter.next();
                    print(recordSet.getKey(), recordSet.getValue());
                }

            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Job History Server Stats for the JMX url.").append("\n");


        System.out.println(sb.toString());
    }


}
