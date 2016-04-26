package com.dstreev.hadoop.yarn;

import com.dstreev.hadoop.hdfs.shell.command.Constants;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.dstreev.hadoop.mapreduce.MRJobRecordConverter;
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
 * Using the Job History Server URL, collect the stats on jobs since the last time this was run or up to
 * 'n' (limit).
 */
public class QueueStats extends HdfsAbstract {
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

    public QueueStats(String name) {
        super(name);
    }

    public QueueStats(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public QueueStats(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public QueueStats(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public QueueStats(String name, Environment env) {
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

        System.out.println("Beginning 'Job History Stat' collection.");

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


        String hostAndPort = configuration.get("mapreduce.jobhistory.webapp.address");

        System.out.println("Job History Server URL: " + hostAndPort);

        String rootPath = hostAndPort + "/ws/v1/history/mapreduce/jobs";

        List<String> queries = getQueries(cmd);

        for (String query: queries) {
            try {
                System.out.println("Query: " + query);
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

                    Map<String, String> jobDetailMap = mrc.jobDetail(jobJson);
                    addRecord("job", jobDetailMap);

                    // Job Counter
                    URL jobCounter = new URL("http://" + rootPath + "/" + jobId + "/counters");
                    URLConnection jobCounterConnection = jobCounter.openConnection();
                    String jobCounterJson = IOUtils.toString(jobCounterConnection.getInputStream());

                    Map<String, String> jobCounterMap = mrc.jobCounters(jobCounterJson);
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

                        Map<String, String> taskDetailMap = mrc.taskDetail(jobId, taskJson);
                        addRecord("task", taskDetailMap);

                        // Get Task Counters <api>/<job_id>/task/<task_id>/counters
                        URL taskCounterUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks/" + taskId + "/counters");
                        URLConnection taskCounterConnection = taskCounterUrl.openConnection();
                        String taskCounterJson = IOUtils.toString(taskCounterConnection.getInputStream());

                        Map<String, String> taskCounterMap = mrc.taskCounter(jobId, taskCounterJson);
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

                            Map<String, String> taskAttemptMap = mrc.attemptDetail(jobId, taskId, taskAttemptJson);
                            addRecord("taskAttempt", taskAttemptMap);

                            // Task Attempt Counter
                            URL taskAttemptCountersUrl = new URL("http://" + rootPath + "/" + jobId + "/tasks/" + taskId + "/attempts/" + taskAttemptId + "/counters");
                            URLConnection taskAttemptCountersConnection = taskAttemptCountersUrl.openConnection();
                            String taskAttemptCounterJson = IOUtils.toString(taskAttemptCountersConnection.getInputStream());

                            Map<String, String> taskAttemptCounterMap = mrc.attemptCounter(jobId, taskId, taskAttemptCounterJson);
                            addRecord("taskAttemptCounter", taskAttemptCounterMap);

                        }
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
