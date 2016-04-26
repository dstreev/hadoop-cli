package com.dstreev.hadoop;

import com.dstreev.hadoop.hdfs.shell.command.Constants;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.dstreev.hadoop.util.HdfsWriter;
import com.dstreev.hadoop.util.RecordConverter;
import com.dstreev.hadoop.yarn.YarnAppRecordConverter;
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
public abstract class AbstractStats extends HdfsAbstract {
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

    public AbstractStats(String name) {
        super(name);
    }

    public AbstractStats(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public AbstractStats(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public AbstractStats(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public AbstractStats(String name, Environment env) {
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
    public abstract void execute(Environment environment, CommandLine cmd, ConsoleReader consoleReader);

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
