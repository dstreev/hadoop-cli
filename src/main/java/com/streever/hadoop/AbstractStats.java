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
import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.CommandReturn;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
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
    protected ObjectMapper mapper = new ObjectMapper();

    protected Configuration configuration = null;

    protected FSDataOutputStream outFS = null;
    protected String baseOutputDir = null;
    protected Boolean ssl = Boolean.FALSE;
    protected Boolean raw = Boolean.FALSE;

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

    public String getProtocol() {
        if (ssl) {
            return "https://";
        } else {
            return "http://";
        }
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
    public final CommandReturn implementation(Environment environment, CommandLine cmd, CommandReturn commandReturn) {
        if (cmd.hasOption("help")) {
            getHelp();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getName(), getOptions());
            return commandReturn;
        }

        // Get the Filesystem
        configuration = (Configuration) env.getValue(Constants.CFG);

        if (cmd.hasOption("ssl")) {
            ssl = Boolean.TRUE;
        } else {
            ssl = Boolean.FALSE;
        }

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
//                baseOutputDir = pathBuilder.resolveFullPath(fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length()), cmd.getOptionValue("output"));
                baseOutputDir = pathBuilder.resolveFullPath(env.getRemoteWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length()), cmd.getOptionValue("output"));
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

            if (cmd.hasOption("raw")) {
                this.raw = Boolean.TRUE;
            }

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // Default Behaviour
            // Set Start Time to previous day IF no config is specified.
            Calendar startCal = Calendar.getInstance();
            Date startDate = new Date(); // default today.
            if (cmd.hasOption("last")) {
                String lastOption = cmd.getOptionValue("last");
                String[] lastParts = lastOption.split("-");
                if (lastParts.length == 2 && NumberUtils.isCreatable(lastParts[0])) {
                    Integer window = Integer.parseInt(lastParts[0]);
                    if (lastParts[1].toUpperCase().startsWith("MIN")) {
                        startCal.add(Calendar.MINUTE, (-1 * window));
                        increment = 60l * 1000l;
                    } else if (lastParts[1].toUpperCase().startsWith("HOUR")) {
                        startCal.add(Calendar.HOUR, (-1 * window));
                        increment = 10l * 60l * 1000l; // ten minutes
                    } else if (lastParts[1].toUpperCase().startsWith("DAY")) {
                        startCal.add(Calendar.DAY_OF_MONTH, (-1 * window));
                        increment = 60l * 60l * 1000l; // 1 hour
                    } else {
                        // bad.
                        System.err.println("last option can't be parsed");
                        throw new RuntimeException("stat option 'l|last' can't be parsed");
                    }
                } else {
                    System.err.println("last option can't be parsed");
                    throw new RuntimeException("stat option 'l|last' can't be parsed");
                }
                startDate = startCal.getTime();
            } else if (cmd.hasOption("start")) {
                if (cmd.hasOption("start")) {
                    try {
                        startDate = df.parse(cmd.getOptionValue("start"));
                    } catch (ParseException e) {
                        e.printStackTrace();
                        commandReturn.setCode(CODE_BAD_DATE);
                        commandReturn.getErr().print(e.getMessage());
                        return commandReturn;
                    }
                }
            } else {
                // default is 1 day.
                startCal.add(Calendar.DAY_OF_MONTH, -1);
                startDate = startCal.getTime();
            }

            // TODO: Need to work in 'current'
            // Set the startTime
            startTime = startDate.getTime();

            if (cmd.hasOption("end")) {
                Date endDate = null;
                try {
                    endDate = df.parse(cmd.getOptionValue("end"));
                } catch (ParseException e) {
                    commandReturn.setCode(CODE_BAD_DATE);
                    commandReturn.getErr().print(e.getMessage());
                    return commandReturn;
//                    e.printStackTrace();
//                    return new CommandReturn(CODE_BAD_DATE, e.getMessage()); // Bad Date
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
            commandReturn.setCode(CODE_STATS_ISSUE);
            commandReturn.getErr().print(t.getMessage());
            return commandReturn;
//            t.printStackTrace();
//            return new CommandReturn(CODE_STATS_ISSUE, t.getMessage());
        }
        return commandReturn;
    }

    public abstract void process(CommandLine cmdln);

    protected void print(String recordSet, String[] fields, List<Map<String, Object>> records) {
        //System.out.println("Record set: " + recordSet);
        int i = 0;
        try {
            StringBuilder sb = new StringBuilder();
            if (header)
                sb.append(StringUtils.join(fields, delimiter)).append("\n");
            for (Map<String, Object> record : records) {
                i++;
                if (i % 8000 == 0)
                    System.out.println(".");
                else if (i % 100 == 0)
                    System.out.print(".");

                String recordStr = RecordConverter.mapToRecord(fields, record, delimiter);

                sb.append(recordStr).append("\n");
            }
            // If the options say to write to hdfs.
            if (baseOutputDir != null) {
                String outputFilename = dfFile.format(new Date()) + ".txt";
                HdfsWriter writer = new HdfsWriter(fs, baseOutputDir + "/" + recordSet.toLowerCase() + "/" + outputFilename);
                writer.append(sb.toString().getBytes());
            } else {
                System.out.println(sb.toString());
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }

    }

    protected Map<String, String> getQueries(CommandLine cmd) {
        Map<String, String> rtn = new LinkedHashMap<String, String>();
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
                rtn.put(sb.toString(), sb2.toString());
            }
        }

        return rtn;
    }

    protected abstract void getHelp();

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        Option helpOption = new Option("h", "help", false, "Help");
        helpOption.setRequired(false);
        opts.addOption(helpOption);

        Option formatOption = new Option("ff", "fileFormat", true,
                "Output filename format.  Value must be a pattern of 'SimpleDateFormat' format options.");
        formatOption.setRequired(false);
        opts.addOption(formatOption);

        Option sslOption = new Option("ssl", "ssl", false,
                "https connection");
        sslOption.setRequired(false);
        opts.addOption(sslOption);

        OptionGroup beginOptionGroup = new OptionGroup();
        Option startOption = new Option("s", "start", true,
                "Start time for retrieval in 'yyyy-MM-dd HH:mm:ss'");
        startOption.setRequired(false);
        beginOptionGroup.addOption(startOption);

        Option lastOption = new Option("l", "last", true,
                "last x-DAY(S)|x-HOUR(S)|x-MIN(S). 1-HOUR=1 hour, 2-DAYS=2 days, 3-HOURS=3 hours, etc.");
        lastOption.setRequired(false);
        beginOptionGroup.addOption(lastOption);

        opts.addOptionGroup(beginOptionGroup);

        // TODO: WIP for current stats.
//        Option currentOption = new Option("c", "current", false, "Get Current / Active Records");
//        currentOption.setRequired(false);
//        beginOptionGroup.addOption(currentOption);

        Option endOption = new Option("e", "end", true,
                "End time for retrieval in 'yyyy-MM-dd HH:mm:ss'");
        endOption.setRequired(false);
        opts.addOption(endOption);

        OptionGroup formatGroup = new OptionGroup();

        Option delimiterOption = new Option("d", "delimiter", true,
                "Record Delimiter (Cntrl-A is default).");
        delimiterOption.setRequired(false);
        formatGroup.addOption(delimiterOption);


        // TODO: Need to implement.
        Option rawOption = new Option("raw", "raw", false,
                "Raw Record Output");
        rawOption.setRequired(false);
        formatGroup.addOption(rawOption);

        opts.addOptionGroup(formatGroup);

        Option headerOption = new Option("hdr", "header", false, "Print Record Header");
        headerOption.setRequired(false);
        opts.addOption(headerOption);


        Option incOption = new Option("inc", "increment", true, "Query Increment in minutes");
        incOption.setRequired(false);
        opts.addOption(incOption);

//        Option schemaOption = new Option("sch", "schema", false, "wip - Print Record Schema");
//        schemaOption.setRequired(false);
//        opts.addOption(schemaOption);

        Option outputOption = new Option("o", "output", true,
                "Output Base Directory (HDFS) (default System.out) from which all other sub-directories are based.");
        outputOption.setRequired(false);
        opts.addOption(outputOption);

//        Option cfOption = new Option("cf", "controlfile", true,
//                "wip - Control File use to track run iterations");
//        cfOption.setRequired(false);
//        opts.addOption(cfOption);

        return opts;
    }

    protected String getInternalRMAddress(String rmId) {
        String rmAddress = null;
        if (ssl) {
            rmAddress = configuration.get("yarn.resourcemanager.webapp.https.address." + rmId);
        } else {
            rmAddress = configuration.get("yarn.resourcemanager.webapp.http.address." + rmId);
            if (rmAddress == null) {
                // Legacy
                rmAddress = configuration.get("yarn.resourcemanager.webapp.address." + rmId);
            }
        }
        if (rmAddress == null) {
            throw new RuntimeException("Could locate RM Web Address, check protocol");
        } else {
            rmAddress = getProtocol() + rmAddress;
        }
//        System.out.println("Checking Resource Manager Endpoint: " + rmAddress);
        return rmAddress;
    }

    protected String getRMState(String urlStr) {
        String rtn = null;
        try {
            URL infoUrl = new URL(urlStr + "/ws/v1/cluster/info");
            URLConnection infoConnection = infoUrl.openConnection();
            String infoJson = IOUtils.toString(infoConnection.getInputStream());
            JsonNode info = mapper.readValue(infoJson, JsonNode.class);
            JsonNode infoNode = info.get("clusterInfo");
            JsonNode haStateNode = infoNode.get("haState");
            rtn = haStateNode.asText();
            System.out.println("RM: " + urlStr + " state: " + rtn);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException("Failed to connect to RM at " + urlStr + ". Check Protocol.", ioe);
        }
        return rtn;
    }

    protected String getActiveRMAddress() {

        String[] rmIds = configuration.get("yarn.resourcemanager.ha.rm-ids").split(",");
        // Get the Host and Port Address using the first id.
        // Is SSL?
//        System.out.println("RM Ids: " + rmIds[0]);
        // Look at the first RM's Info and check for Active.
        String rmAddress = getInternalRMAddress(rmIds[0]);
        if (!getRMState(rmAddress).equals("ACTIVE")) {
            rmAddress = getInternalRMAddress(rmIds[1]);
            if (!getRMState(rmAddress).equals("ACTIVE")) {
                throw new RuntimeException("Could locate ACTIVE Resource Manager");
            }
        }
        return rmAddress;
    }

    public String getResourceManagerWebAddress() {
        // Check for HA.
        // yarn.resourcemanager.ha.enabled=true
        String rmAddress = null;
        String ha = configuration.get("yarn.resourcemanager.ha.enabled");
        if (ha != null && Boolean.parseBoolean(ha)) {
            // Get the RM id's
            rmAddress = getActiveRMAddress();
        } else {
            // Non HA
            // Is SSL?
            if (ssl) {
                rmAddress = getProtocol() + configuration.get("yarn.resourcemanager.webapp.https.address");
            } else {
                rmAddress = getProtocol() + configuration.get("yarn.resourcemanager.webapp.http.address");
                if (rmAddress == null) {
                    // Legacy
                    rmAddress = getProtocol() + configuration.get("yarn.resourcemanager.webapp.address");
                }
            }
        }
        return rmAddress;
    }

}
