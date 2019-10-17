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

package com.streever.hadoop;

import com.streever.hadoop.hdfs.shell.command.Constants;
import com.streever.hadoop.hdfs.shell.command.Direction;
import com.streever.hadoop.util.HdfsWriter;
import com.streever.hadoop.util.RecordConverter;
import com.streever.hadoop.hdfs.shell.command.HdfsAbstract;
import com.streever.tools.stemshell.Environment;
import com.streever.tools.stemshell.command.CommandReturn;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the stats on applications since the last time this was run or up to
 * 'n' (limit).
 */
public abstract class AbstractStats extends HdfsAbstract {
    protected Configuration configuration = null;

    protected FSDataOutputStream outFS = null;
    protected String baseOutputDir = null;

    protected DistributedFileSystem fs = null;

    protected static String DEFAULT_FILE_FORMAT = "yyyy-MM";

    protected DateFormat dfFile = null;

    protected Map<String, List<Map<String, Object>>> records = new LinkedHashMap<String, List<Map<String, Object>>>();
    protected Boolean header = Boolean.FALSE;

    protected Long increment = 60l * 60l * 1000l; // 1 hour

    protected static final String DEFAULT_DELIMITER = "\u0001";
    protected String delimiter = DEFAULT_DELIMITER;

    /**
     * The earliest start time to get available jobs. Time since Epoch...
     */
    protected Long startTime = 0l;
    protected Long endTime = 0l;

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

    public List<Map<String, Object>> getRecordList(String recordType) {
        List<Map<String, Object>> rtn = records.get(recordType);
        return rtn;
    }

    public void clearCache() {
        records.clear();
    }

    public Map<String, List<Map<String, Object>>> getRecords() {
        return records;
    }

    public void addRecord(String recordType, Map<String, Object> record) {
        List<Map<String, Object>> list = null;
        if (records.containsKey(recordType)) {
            list = records.get(recordType);
        } else {
            list = new ArrayList<Map<String, Object>>();
            records.put(recordType, list);
        }
        list.add(record);
    }

    public void addRecords(String recordType, List<Map<String, Object>> inRecords) {
        List<Map<String, Object>> list = null;
        if (records.containsKey(recordType)) {
            list = records.get(recordType);
        } else {
            list = new ArrayList<Map<String, Object>>();
            records.put(recordType, list);
        }
        list.addAll(inRecords);
    }

    @Override
    public final CommandReturn implementation(Environment environment, CommandLine cmd, ConsoleReader consoleReader) {
        if (cmd.hasOption("help")) {
            getHelp();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hadoop-cli <stats>", getOptions());
            return CommandReturn.GOOD;
        }

        // Get the Filesystem
        configuration = (Configuration) env.getValue(Constants.CFG);

        try {

            fs = (DistributedFileSystem) env.getValue(Constants.HDFS);


            Option[] cmdOpts = cmd.getOptions();
            String[] cmdArgs = cmd.getArgs();

            if (cmd.hasOption("fileFormat")) {
                dfFile = new SimpleDateFormat(cmd.getOptionValue("fileFormat"));
            } else {
                dfFile = new SimpleDateFormat(DEFAULT_FILE_FORMAT);
            }

            if (cmd.hasOption("output")) {
                // Get a handle to the FileSystem if we intent to write our results to the HDFS.
                baseOutputDir = pathBuilder.resolveFullPath(fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length()), cmd.getOptionValue("output"));
            } else {
                baseOutputDir = null;
            }

            if (cmd.hasOption("header")) {
                this.header = Boolean.TRUE;
            } else {
                this.header = Boolean.FALSE;
            }

            if (cmd.hasOption("delimiter")) {
                this.delimiter = cmd.getOptionValue("delimiter");
            } else {
                this.delimiter = DEFAULT_DELIMITER;
            }

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (cmd.hasOption("start")) {
                Date startDate = null;
                try {
                    startDate = df.parse(cmd.getOptionValue("start"));
                } catch (ParseException e) {
                    e.printStackTrace();
                    return new CommandReturn(CODE_BAD_DATE, e.getMessage()); // Bad Date
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
                    return new CommandReturn(CODE_BAD_DATE, e.getMessage()); // Bad Date
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

            clearCache();

            process(cmd);

            clearCache();
        } catch (Throwable t) {
            t.printStackTrace();
            return new CommandReturn(CODE_STATS_ISSUE, t.getMessage());
        }
        return CommandReturn.GOOD;
    }

    public abstract void process(CommandLine cmdln);

    protected void print(String recordSet, List<Map<String, Object>> records) {
        //System.out.println("Record set: " + recordSet);
        int i = 0;
        try {
            StringBuilder sb = new StringBuilder();
            for (Map<String, Object> record : records) {
                i++;
                if (i % 8000 == 0)
                    System.out.println(".");
                else if (i % 100 == 0)
                    System.out.print(".");

                String recordStr = RecordConverter.mapToRecord(record, header, delimiter);

                if (header) {
                    sb.append(recordStr);
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
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }

    }

    protected Map<String,String> getQueries(CommandLine cmd) {
        Map<String,String> rtn = new LinkedHashMap<String,String>();
        Long begin = startTime;
        Long end = endTime;

        if (begin + increment < end) {
            while (begin < end) {
                StringBuilder sb = new StringBuilder();
                StringBuilder sb2 = new StringBuilder();
                sb.append("finishedTimeBegin=").append(begin);
                sb2.append("finishedTimeBegin=").append(new Date(begin));
                begin = begin + increment - 1;
                sb.append("&finishedTimeEnd=").append(begin);
                sb2.append("&finishedTimeEnd=").append(new Date(begin));
                begin += 1;
                rtn.put(sb.toString(),sb2.toString());
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

        Option delimiterOption = Option.builder("d").required(false)
                .argName("delimiter")
                .desc("Record Delimiter (Cntrl-A is default).")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("delimiter")
                .build();
        opts.addOption(delimiterOption);

        Option headerOption = Option.builder("hdr").required(false)
                .argName("header")
                .desc("Print Record Header")
                .longOpt("header")
                .build();
        opts.addOption(headerOption);

        Option currentOption = Option.builder("c").required(false)
                .argName("current")
                .desc("Get Current / Active Records")
                .longOpt("current")
                .build();
        opts.addOption(currentOption);

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
                .desc("wip - Print Record Schema")
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
                .desc("wip - Control File use to track run iterations")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("controlfile")
                .build();
        opts.addOption(cfOption);

        return opts;
    }

}
