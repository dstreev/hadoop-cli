package com.cloudera.utils.hadoop.yarn;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class ResourceManagerStats implements Stats {

    protected ObjectMapper mapper = new ObjectMapper();

    protected Configuration configuration = null;

    protected Boolean ssl = Boolean.FALSE;
    protected Boolean raw = Boolean.FALSE;

    protected Long increment = 60l * 60l * 1000l; // 1 hour

    /**
     * The earliest start time to get available jobs. Time since Epoch...
     */
    protected Long startTime = 0l;
    protected Long endTime = 0l;

    protected Map<String, List<Map<String, Object>>> records = new LinkedHashMap<String, List<Map<String, Object>>>();

    private Options options;

    public Boolean getSsl() {
        return ssl;
    }

    public void setSsl(Boolean ssl) {
        this.ssl = ssl;
    }

    public Boolean getRaw() {
        return raw;
    }

    public void setRaw(Boolean raw) {
        this.raw = raw;
    }

    public Long getIncrement() {
        return increment;
    }

    public void setIncrement(Long increment) {
        this.increment = increment;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public ResourceManagerStats(Configuration configuration) {
        setConfiguration(configuration);
    }

    public ResourceManagerStats() {
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
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

    public void init(CommandLine cmd) {
//        cr = super.processOptions(environment, cmd, cr);

        try {

            Option[] cmdOpts = cmd.getOptions();
            String[] cmdArgs = cmd.getArgs();

            if (cmd.hasOption("ssl")) {
                ssl = Boolean.TRUE;
            } else {
                ssl = Boolean.FALSE;
            }

            if (cmd.hasOption("raw")) {
                this.raw = Boolean.TRUE;
            } else {
                this.raw = Boolean.FALSE;
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
//                        cr.setCode(CODE_BAD_DATE);
//                        cr.getErr().print(e.getMessage());
//                        return cr;
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
//                    cr.setCode(CODE_BAD_DATE);
//                    cr.getErr().print(e.getMessage());
//                    return cr;
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
        } catch (Throwable t) {
//            cr.setCode(CODE_STATS_ISSUE);
//            cr.getErr().print(t.getMessage());
//            return cr;
        }
//        return cr;
    }

    public String getProtocol() {
        if (ssl) {
            return "https://";
        } else {
            return "http://";
        }
    }

    protected Map<String, String> getQueries() {
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

    public Options getOptions() {
        Options opts = new Options();

        Option sslOption = new Option("ssl", "ssl", false,
                "https connection");
        sslOption.setRequired(false);
        opts.addOption(sslOption);

        Option rawOption = new Option("raw", "raw", false,
                "Raw Record Output");
        rawOption.setRequired(false);
        opts.addOption(rawOption);

        return opts;
    }
}
