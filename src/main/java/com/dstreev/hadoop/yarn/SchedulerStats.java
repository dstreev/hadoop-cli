package com.dstreev.hadoop.yarn;

import com.dstreev.hadoop.AbstractStats;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.dstreev.hadoop.yarn.parsers.QueueParser;
import com.instanceone.stemshell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by dstreev on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the queue stats .
 *
 */
public class SchedulerStats extends AbstractStats {

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

        String hostAndPort = configuration.get("yarn.resourcemanager.webapp.address");

        System.out.println("Resource Manager Server URL: " + hostAndPort);

        String rootPath = hostAndPort + "/ws/v1/cluster/scheduler";

        try {

            URL schUrl = new URL("http://" + rootPath);

            URLConnection schConnection = schUrl.openConnection();
            String schJson = IOUtils.toString(schConnection.getInputStream());

            QueueParser queueParser = new QueueParser(schJson);
            Map<String, List<Map<String,Object>>> queueSet = queueParser.getQueues(timestamp);

            Iterator<Map.Entry<String,List<Map<String,Object>>>> qIter = queueSet.entrySet().iterator();

            while (qIter.hasNext()) {
                Map.Entry<String, List<Map<String,Object>>> entry = qIter.next();
                addRecords(entry.getKey(), entry.getValue());
            }

                /*
                1. Get current queue state.

                 */


            Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getRecords().entrySet().iterator();
            while (rIter.hasNext()) {
                Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
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
