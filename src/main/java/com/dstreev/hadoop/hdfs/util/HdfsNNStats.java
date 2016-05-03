package com.dstreev.hadoop.hdfs.util;

import com.dstreev.hadoop.AbstractStats;
import com.dstreev.hadoop.hdfs.shell.command.Constants;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.instanceone.hdfs.shell.command.HdfsAbstract;
import com.instanceone.stemshell.Environment;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by dstreev on 2016-02-15.
 * <p>
 * The intent here is to provide a means of querying the Namenode and
 * producing Metadata about the directory AND the files in it.
 */
public class HdfsNNStats extends AbstractStats {

    public HdfsNNStats(String name) {
        super(name);
    }

    public HdfsNNStats(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public HdfsNNStats(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public HdfsNNStats(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public HdfsNNStats(String name, Environment env) {
        super(name, env);
    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Namenode stats from the available Namenode JMX url's.").append("\n");
        sb.append("").append("\n");
        sb.append("3 Type of stats are current collected and written to hdfs (with -o option) or to screen (no option specified)").append("\n");
        sb.append("The 'default' delimiter for all records is '\u0001' (Cntl-A)").append("\n");
        sb.append("").append("\n");
        sb.append(">> Namenode Information: (optionally written to the directory 'nn_info')").append("\n");
        sb.append("Fields: Timestamp, HostAndPort, State, Version, Used, Free, Safemode, TotalBlocks, TotalFiles, NumberOfMissingBlocks, NumberOfMissingBlocksWithReplicationFactorOne").append("\n");
        sb.append("").append("\n");
        sb.append(">> Filesystem State: (optionally written to the directory 'fs_state')").append("\n");
        sb.append("Fields: Timestamp, HostAndPort, State, CapacityUsed, CapacityRemaining, BlocksTotal, PendingReplicationBlocks, UnderReplicatedBlocks, ScheduledReplicationBlocks, PendingDeletionBlocks, FSState, NumLiveDataNodes, NumDeadDataNodes, NumDecomLiveDataNodes, NumDecomDeadDataNodes, VolumeFailuresTotal").append("\n");
        sb.append("").append("\n");
        sb.append(">> Top User Operations: (optionally written to the directory 'top_user_ops')").append("\n");
        sb.append("Fields: Timestamp, HostAndPort, State, WindowLenMs, Operation, User, Count").append("\n");
        sb.append("").append("\n");
        System.out.println(sb.toString());
    }

    @Override
    public void process(CommandLine cmd) {

//        // Find the hdfs http urls.
        Map<URL, Map<NamenodeJmxBean, URL>> namenodeJmxUrls = getNamenodeHTTPUrls(configuration);

        // For each URL.
        for (Map.Entry<URL, Map<NamenodeJmxBean, URL>> entry : namenodeJmxUrls.entrySet()) {
            try {
//                System.out.println("Checking URL: " + entry.getKey());
                URLConnection statusConnection = entry.getKey().openConnection();
                String statusJson = IOUtils.toString(statusConnection.getInputStream());


                for (Map.Entry<NamenodeJmxBean, URL> innerEntry : entry.getValue().entrySet()) {
//                    System.out.println(innerEntry.getKey() + ": " + innerEntry.getValue());

                    URLConnection httpConnection = innerEntry.getValue().openConnection();
                    String beanJson = IOUtils.toString(httpConnection.getInputStream());

                    NamenodeJmxParser njp = null;

                    njp = new NamenodeJmxParser(statusJson, beanJson);

                    // URL Query should match key.
                    switch (innerEntry.getKey()) {
                        case NN_INFO_JMX_BEAN:
                            // Get and Save NN Info.
                            Map<String, Object> nnInfo = njp.getNamenodeInfo();
                            addRecord("nn_info", nnInfo);
                            break;
                        case FS_STATE_JMX_BEAN:
                            List<Map<String, Object>> topUserOps = njp.getTopUserOpRecords();
                            // Get and Save TopUserOps
                            addRecords("top_user_ops", topUserOps);

                            // Get and Save FS State
                            Map<String,Object> fsState = njp.getFSState();
                            addRecord("fs_state", fsState);
                            break;
                    }


                }
            } catch (Throwable e) {
                e.printStackTrace();
            }

        }

        Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getRecords().entrySet().iterator();
        while (rIter.hasNext()) {
            Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
            print(recordSet.getKey(), recordSet.getValue());
        }
        // Clear for next query
        clearCache();

    }

    private Map<URL, Map<NamenodeJmxBean, URL>> getNamenodeHTTPUrls(Configuration configuration) {
        Map<URL, Map<NamenodeJmxBean, URL>> rtn = new LinkedHashMap<URL, Map<NamenodeJmxBean, URL>>();

        // Determine if HA is enabled.
        // Look for 'dfs.nameservices', if present, then HA is enabled.
        String nameServices = configuration.get("dfs.nameservices");
        if (nameServices != null) {
            // HA Enabled
            String[] nnRefs = configuration.get("dfs.ha.namenodes." + nameServices).split(",");

            // Get the http addresses.
            for (String nnRef : nnRefs) {
                String hostAndPort = configuration.get("dfs.namenode.http-address." + nameServices + "." + nnRef);
                if (hostAndPort != null) {
                    try {
                        URL statusURL = new URL("http://" + hostAndPort + "/jmx?qry=" + NamenodeJmxParser.NN_STATUS_JMX_BEAN);
                        rtn.put(statusURL, getURLMap(hostAndPort));
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            // Standalone
            String hostAndPort = configuration.get("dfs.namenode.http-address");
            try {
                URL statusURL = new URL("http://" + hostAndPort + "/jmx?qry=" + NamenodeJmxParser.NN_STATUS_JMX_BEAN);
                rtn.put(statusURL, getURLMap(hostAndPort));
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        return rtn;
    }

    private Map<NamenodeJmxBean, URL> getURLMap(String hostAndPort) {
        Map<NamenodeJmxBean, URL> rtn = new LinkedHashMap<>();
        try {
            for (NamenodeJmxBean target_bean : NamenodeJmxBean.values()) {
                rtn.put(target_bean, new URL("http://" + hostAndPort + "/jmx?qry=" + target_bean.getBeanName()));
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return rtn;
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

        Option outputOption = Option.builder("o").required(false)
                .argName("output")
                .desc("Output Base Directory (HDFS) (default System.out) from which all other sub-directories are based.")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("output")
                .build();
        opts.addOption(outputOption);

        return opts;
    }

}