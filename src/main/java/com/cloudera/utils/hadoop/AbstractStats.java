
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

package com.cloudera.utils.hadoop;

import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.hdfs.shell.command.Direction;
import com.cloudera.utils.hadoop.util.HdfsWriter;
import com.cloudera.utils.hadoop.util.RecordConverter;
import com.cloudera.utils.hadoop.hdfs.shell.command.HdfsAbstract;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
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

    private DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

    protected DistributedFileSystem fs = null;

    protected static String DEFAULT_FILE_FORMAT = "yyyy-MM";

    protected DateFormat dfFile = null;

    protected Map<String, List<Map<String, Object>>> records = new LinkedHashMap<String, List<Map<String, Object>>>();
    protected Boolean header = Boolean.FALSE;

    public static final String DEFAULT_DELIMITER = "\u0001";

    protected String delimiter = DEFAULT_DELIMITER;

    public Boolean isRaw() {
        return raw;
    }

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

    public CommandReturn processOptions(Environment environment, CommandLine cmd, CommandReturn cr) {
//        CommandReturn scr = processOptions(environment, cmd, cr);

        if (cmd.hasOption("help")) {
            getHelp();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getName(), getOptions());
            return cr;
        }

        // Get the Filesystem
        configuration = env.getConfig();

        if (cmd.hasOption("ssl")) {
            ssl = Boolean.TRUE;
        } else {
            ssl = Boolean.FALSE;
        }

        try {

            fs = (DistributedFileSystem) env.getFileSystemOrganizer().getDefaultFileSystemState().getFileSystem();

//            Option[] cmdOpts = cmd.getOptions();
//            String[] cmdArgs = cmd.getArgs();

            if (cmd.hasOption("fileFormat")) {
                dfFile = new SimpleDateFormat(cmd.getOptionValue("fileFormat"));
            } else {
                dfFile = new SimpleDateFormat(DEFAULT_FILE_FORMAT);
            }

            if (cmd.hasOption("output")) {
                baseOutputDir = cmd.getOptionValue("output");
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
            } else {
                this.raw = Boolean.FALSE;
            }
        } catch (Throwable t) {
            cr.setCode(CODE_STATS_ISSUE);
            cr.getErr().print(t.getMessage());
        }
        return cr;
    }

    @Override
    public CommandReturn implementation(Environment environment, CommandLine cmd, CommandReturn cr) {
        processOptions(environment, cmd, cr);
        try {

            clearCache();

            process(cmd);

            clearCache();
        } catch (Throwable t) {
            cr.setCode(CODE_STATS_ISSUE);
            cr.getErr().print(t.getMessage());
        }
        return cr;
    }

    public abstract void process(CommandLine cmdln);

    protected void print(String recordSet, String rawRecord) {
        // If the options say to write to hdfs.
        if (baseOutputDir != null) {
            StringBuilder sb = new StringBuilder();
            String timestamp = dateTimeFormat.format(new Date());
            sb.append(timestamp).append(delimiter);
            sb.append(rawRecord).append("\n");
            String outputFilename = dfFile.format(new Date()) + ".txt";
            HdfsWriter writer = new HdfsWriter(fs, baseOutputDir + "/" + recordSet.toLowerCase() + "/" + outputFilename);
            writer.append(sb.toString().getBytes());
        } else {
            System.out.println(rawRecord);
        }
    }

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

        OptionGroup formatGroup = new OptionGroup();

        Option delimiterOption = new Option("d", "delimiter", true,
                "Record Delimiter (Cntrl-A is default).");
        delimiterOption.setRequired(false);
        opts.addOption(delimiterOption);
//        formatGroup.addOption(delimiterOption);

        // TODO: Need to implement.
        Option rawOption = new Option("raw", "raw", false,
                "Raw Record Output");
        rawOption.setRequired(false);
        opts.addOption(rawOption);
//        formatGroup.addOption(rawOption);

//        opts.addOptionGroup(formatGroup);

        Option headerOption = new Option("hdr", "header", false, "Print Record Header");
        headerOption.setRequired(false);
        opts.addOption(headerOption);

        Option outputOption = new Option("o", "output", true,
                "Output Base Directory (HDFS) (default System.out) from which all other sub-directories are based.");
        outputOption.setRequired(false);
        opts.addOption(outputOption);

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
