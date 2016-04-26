package com.dstreev.hadoop.yarn;

import com.dstreev.hadoop.AbstractStats;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.dstreev.hadoop.yarn.parsers.QueueParser;
import com.instanceone.stemshell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * Created by dstreev on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the stats on applications since the last time this was run or up to
 * 'n' (limit).
 */
public class SchedulerStats extends AbstractStats {

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

        String hostAndPort = configuration.get("yarn.resourcemanager.webapp.address");

        System.out.println("Resource Manager Server URL: " + hostAndPort);

        String rootPath = hostAndPort + "/ws/v1/cluster/scheduler";

        try {

            URL schUrl = new URL("http://" + rootPath);

            URLConnection schConnection = schUrl.openConnection();
            String schJson = IOUtils.toString(schConnection.getInputStream());

            QueueParser queueParser = new QueueParser(schJson);
            Map<String, List<Map<String,String>>> queueSet = queueParser.getQueues();

            Iterator<Map.Entry<String,List<Map<String,String>>>> qIter = queueSet.entrySet().iterator();

            while (qIter.hasNext()) {
                Map.Entry<String, List<Map<String,String>>> entry = qIter.next();
                addRecords(entry.getKey(), entry.getValue());
            }

                /*
                1. Get current queue state.

                 */


            Iterator<Map.Entry<String, List<Map<String, String>>>> rIter = getRecords().entrySet().iterator();
            while (rIter.hasNext()) {
                Map.Entry<String, List<Map<String, String>>> recordSet = rIter.next();
                print(recordSet.getKey(), recordSet.getValue());
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Queue Stats from the JMX url.").append("\n");


        System.out.println(sb.toString());
    }


}
