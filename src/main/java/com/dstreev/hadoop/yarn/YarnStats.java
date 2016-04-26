package com.dstreev.hadoop.yarn;

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
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dstreev on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the stats on applications since the last time this was run or up to
 * 'n' (limit).
 */
public class YarnStats extends HdfsAbstract {
    private Configuration configuration = null;

    private FSDataOutputStream outFS = null;
    private String baseOutputDir = null;

    private DistributedFileSystem fs = null;

    private static String DEFAULT_FILE_FORMAT = "yyyy-MM";

    private DateFormat dfFile = null;

    private Map<String, List<Map<String,String>>> records = new LinkedHashMap<String, List<Map<String,String>>>();
    private Boolean header = Boolean.FALSE;

    private Long increment = 60l * 60l * 1000l; // 1 hour

    /**
     * The earliest start time to get available jobs. Time since Epoch...
     */
    private Long startTime = 0l;
    private Long endTime = 0l;

    public YarnStats(String name) {
        super(name);
    }

    public YarnStats(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public YarnStats(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public YarnStats(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public YarnStats(String name, Environment env) {
        super(name, env);
    }

    public List<Map<String, String>> getRecordList(String recordType) {
        List<Map<String, String>> rtn = records.get(recordType);
        return rtn;
    }

    public void clearCache() {
        records.clear();
    }

    public Map<String, List<Map<String, String>>> getRecords() {
        return records;
    }

    public void addRecord(String recordType, Map<String, String> record) {
        List<Map<String, String>> list = null;
        if (records.containsKey(recordType)) {
            list = records.get(recordType);
        } else {
            list = new ArrayList<Map<String,String>>();
            records.put(recordType, list);
        }
        list.add(record);
    }

    @Override
    public void execute(Environment environment, CommandLine cmd, ConsoleReader consoleReader) {
        if (cmd.hasOption("help")) {
            getHelp();
            return;
        }

        System.out.println("Beginning 'Resource Manager Stat' collection.");

        // Get the Filesystem
        configuration = (Configuration) env.getValue(Constants.CFG);

        fs = (DistributedFileSystem) env.getValue(Constants.HDFS);

        if (fs == null) {
            System.out.println("Please connect first");
            return;
        }

//        URI nnURI = fs.getUri();

        // Find the hdfs http urls.
//        Map<URL, Map<NamenodeJmxBean, URL>> namenodeJmxUrls = getNamenodeHTTPUrls(configuration);

        Option[] cmdOpts = cmd.getOptions();
        String[] cmdArgs = cmd.getArgs();

        if (cmd.hasOption("fileFormat")) {
            dfFile = new SimpleDateFormat(cmd.getOptionValue("fileFormat"));
        } else {
            dfFile = new SimpleDateFormat(DEFAULT_FILE_FORMAT);
        }

        if (cmd.hasOption("output")) {
            baseOutputDir = buildPath2(fs.getWorkingDirectory().toString().substring(((String) env.getProperty(Constants.HDFS_URL)).length()), cmd.getOptionValue("output"));
        } else {
            baseOutputDir = null;
        }

        if (cmd.hasOption("header")) {
            this.header = Boolean.TRUE;
        } else {
            this.header = Boolean.FALSE;
        }

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (cmd.hasOption("start")) {
            Date startDate = null;
            try {
                startDate = df.parse(cmd.getOptionValue("start"));
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
            startTime = startDate.getTime();
        } else {
            // Set Start Time to previous day IF no config is specified.
            Calendar startCal = Calendar.getInstance();
            startCal.add(Calendar.DAY_OF_MONTH, -1);
            Date startDate = startCal.getTime();
            startTime = startDate.getTime();
        }

        if (cmd.hasOption("end")) {
            Date endDate = null;
            try {
                endDate = df.parse(cmd.getOptionValue("end"));
            } catch (ParseException e) {
                e.printStackTrace();
                return;
            }
            endTime = endDate.getTime();
        } else {
            // If no Config.
            // Set to now.
            endTime = new Date().getTime();
        }

        if (cmd.hasOption("increment")) {
            String incStr = cmd.getOptionValue("increment");
            increment = Long.parseLong(incStr) * 60l * 1000l;
        }

        // TODO: Read Interval Config


        String hostAndPort = configuration.get("yarn.resourcemanager.webapp.address");

        System.out.println("Resource Manager Server URL: " + hostAndPort);

        String rootPath = hostAndPort + "/ws/v1/cluster/apps";

        List<String> queries = getQueries(cmd);

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
                // Clear for next query
                clearCache();

            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }


    protected void print(String recordSet, List<Map<String, String>> records) {
        System.out.println("Record set: " + recordSet);
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (Map<String, String> record: records) {
            i++;
            if (i % 8000 == 0)
                System.out.println(".");
            else if (i % 100 == 0)
                System.out.print(".");

                String recordStr = RecordConverter.mapToRecord(record, header, null);
            if (header) {
                System.out.println(recordStr);
                // Terminate Loop.
                break;
            } else {
                sb.append(recordStr).append("\n");
            }
        }
        // If the options say to write to hdfs.
        if (baseOutputDir != null) {
            String outputFilename = dfFile.format(new Date()) + ".txt";
            HdfsWriter writer = new HdfsWriter(fs, baseOutputDir + "/" + recordSet + "/" + outputFilename);
            writer.append(sb.toString().getBytes());
        } else {
            System.out.println(sb.toString());
        }

    }

    protected List<String> getQueries(CommandLine cmd) {
        List<String> rtn = new ArrayList<String>();
        Long begin = startTime;
        Long end = endTime;

        if (begin + increment < end) {
            while (begin < end) {
                StringBuilder sb = new StringBuilder();
                sb.append("finishedTimeBegin=").append(begin);
                begin = begin + increment;
                sb.append("&finishedTimeEnd=").append(begin);
                rtn.add(sb.toString());
            }
        }

        return rtn;
    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Job History Server Stats for the JMX url.").append("\n");


        System.out.println(sb.toString());
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        Option helpOption = Option.builder("h").required(false)
                .argName("help")
                .desc("Help")
                .hasArg(false)
                .longOpt("help")
                .build();
        opts.addOption(helpOption);

        Option formatOption = Option.builder("ff").required(false)
                .argName("fileFormat")
                .desc("Output filename format.  Value must be a pattern of 'SimpleDateFormat' format options.")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("fileFormat")
                .build();
        opts.addOption(formatOption);

        Option startOption = Option.builder("s").required(false)
                .argName("start")
                .desc("Start time for retrieval in 'yyyy-MM-dd HH:mm:ss'")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("start")
                .build();
        opts.addOption(startOption);

        Option endOption = Option.builder("e").required(false)
                .argName("end")
                .desc("End time for retrieval in 'yyyy-MM-dd HH:mm:ss'")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("end")
                .build();
        opts.addOption(endOption);

        Option headerOption = Option.builder("hdr").required(false)
                .argName("header")
                .desc("Print Record Header")
                .longOpt("header")
                .build();
        opts.addOption(headerOption);


        Option incOption = Option.builder("inc").required(false)
                .argName("increment")
                .desc("Query Increment in Minutes")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("increment")
                .build();
        opts.addOption(incOption);

        Option schemaOption = Option.builder("sch").required(false)
                .argName("schema")
                .desc("Print Record Header")
                .longOpt("schema")
                .build();
        opts.addOption(schemaOption);

        Option outputOption = Option.builder("o").required(false)
                .argName("output")
                .desc("Output Base Directory (HDFS) (default System.out) from which all other sub-directories are based.")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("output")
                .build();
        opts.addOption(outputOption);

        Option cfOption = Option.builder("cf").required(false)
                .argName("controlfile")
                .desc("Control File use to track run iterations")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("controlfile")
                .build();
        opts.addOption(cfOption);

        return opts;
    }

}
