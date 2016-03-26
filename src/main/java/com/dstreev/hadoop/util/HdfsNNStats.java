package com.dstreev.hadoop.util;

import com.dstreev.hdfs.shell.command.Constants;
import com.dstreev.hdfs.shell.command.Direction;
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
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dstreev on 2016-02-15.
 * <p>
 * The intent here is to provide a means of querying the Namenode and
 * producing Metadata about the directory AND the files in it.
 */
public class HdfsNNStats extends HdfsAbstract {
    private Configuration configuration = null;
//    private DFSClient dfsClient = null;
    private FSDataOutputStream outFS = null;
    private String baseOutputDir = null;

    private DistributedFileSystem fs = null;

    private static String DEFAULT_FILE_FORMAT = "yyyy-MM";

    private DateFormat dfFile = null;

//    private static String FS_STATE_JMX_BEAN = "Hadoop:service=NameNode,name=FSNamesystemState";
//    private static String NN_INFO_JMX_BEAN = "Hadoop:service=NameNode,name=NameNodeInfo";

//    enum TARGET_BEAN {
//        FS_STATE_JMX_BEAN("Hadoop:service=NameNode,name=FSNamesystemState"),
////        NN_STATUS_JMX_BEAN("Hadoop:service=NameNode,name=NameNodeStatus"),
//        NN_INFO_JMX_BEAN("Hadoop:service=NameNode,name=NameNodeInfo");
//
//        private String beanName;
//
//        public String getBeanName() {
//            return beanName;
//        }
//        private TARGET_BEAN(String beanName) {
//            this.beanName = beanName;
//        }
//    }

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
    public void execute(Environment environment, CommandLine cmd, ConsoleReader consoleReader) {

        if (cmd.hasOption("help")) {
            getHelp();
            return;
        }

        System.out.println("Beginning 'Namenode Stat' collection.");

        // Get the Filesystem
        configuration = (Configuration) env.getValue(Constants.CFG);

        fs = (DistributedFileSystem) env.getValue(Constants.HDFS);

        if (fs == null) {
            System.out.println("Please connect first");
            return;
        }

        URI nnURI = fs.getUri();

        // Find the hdfs http urls.
        Map<URL, Map<NamenodeJmxBean, URL>> namenodeJmxUrls = getNamenodeHTTPUrls(configuration);

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

        // For each URL.
        for (Map.Entry<URL, Map<NamenodeJmxBean, URL>> entry : namenodeJmxUrls.entrySet()) {
            try {

                URLConnection statusConnection = entry.getKey().openConnection();
                String statusJson = IOUtils.toString(statusConnection.getInputStream());


                for (Map.Entry<NamenodeJmxBean, URL> innerEntry : entry.getValue().entrySet()) {
//                    System.out.println(entry.getKey() + ": " + innerEntry.getValue());

                    URLConnection httpConnection = innerEntry.getValue().openConnection();
                    String beanJson = IOUtils.toString(httpConnection.getInputStream());

                    NamenodeJmxParser njp = null;

                    String outputFilename = dfFile.format(new Date()) + ".txt";
                    njp = new NamenodeJmxParser(statusJson, beanJson);

                    // URL Query should match key.
                    switch (innerEntry.getKey()) {
                        case NN_INFO_JMX_BEAN:
                            // Get and Save NN Info.
                            String nnInfo = njp.getNamenodeInfo();
                            //      Open NN Info File.
                            if (baseOutputDir != null) {
                                String nnInfoPathStr = null;
                                if (baseOutputDir.endsWith("/")) {
                                    nnInfoPathStr = baseOutputDir + "nn_info/" + outputFilename;
                                } else {
                                    nnInfoPathStr = baseOutputDir + "/nn_info/" + outputFilename;
                                }

                                Path nnInfoPath = new Path(nnInfoPathStr);
                                FSDataOutputStream nnInfoOut = null;
                                if (fs.exists(nnInfoPath)) {
//                            System.out.println("NN Info APPENDING");
                                    nnInfoOut = fs.append(nnInfoPath);
                                } else {
//                            System.out.println("NN Info CREATING");
                                    nnInfoOut = fs.create(nnInfoPath);
                                }
                                nnInfoOut.write(nnInfo.getBytes());
                                // Newline
                                nnInfoOut.write("\n".getBytes());
                                nnInfoOut.close();
                            } else {
                                System.out.println(">> Namenode Info: ");
                                System.out.println(nnInfo);
                            }
                            break;
                        case FS_STATE_JMX_BEAN:
                            List<String> topUserOps = njp.getTopUserOpRecords();
                            // Get and Save TopUserOps
                            //      Open TopUserOps file.
                            if (topUserOps.size() > 0) {
                                if (baseOutputDir != null) {
                                    String topUserOpsPathStr = null;
                                    if (baseOutputDir.endsWith("/")) {
                                        topUserOpsPathStr = baseOutputDir + "top_user_ops/" + outputFilename;
                                    } else {
                                        topUserOpsPathStr = baseOutputDir + "/top_user_ops/" + outputFilename;
                                    }

                                    Path topUserOpsPath = new Path(topUserOpsPathStr);
                                    FSDataOutputStream topUserOpsOut = null;
                                    if (fs.exists(topUserOpsPath)) {
//                                System.out.println("Top User Ops APPENDING");
                                        topUserOpsOut = fs.append(topUserOpsPath);
                                    } else {
//                                System.out.println("Top User Ops CREATING");
                                        topUserOpsOut = fs.create(topUserOpsPath);
                                    }
                                    for (String record : topUserOps) {
                                        topUserOpsOut.write(record.getBytes());
                                        // Newline
                                        topUserOpsOut.write("\n".getBytes());

                                    }
                                    topUserOpsOut.close();
                                } else {
                                    System.out.println(">> Top User Operations: ");
                                    for (String record : topUserOps) {
                                        System.out.println(record);
                                    }
                                }
                            }

                            // Get and Save FS State
                            String fsState = njp.getFSState();
                            //      Open FS State Stat File.
                            if (baseOutputDir != null) {
                                String fsStatePathStr = null;
                                if (baseOutputDir.endsWith("/")) {
                                    fsStatePathStr = baseOutputDir + "fs_state/" + outputFilename;
                                } else {
                                    fsStatePathStr = baseOutputDir + "/fs_state/" + outputFilename;
                                }

                                Path fsStatePath = new Path(fsStatePathStr);
                                FSDataOutputStream fsStateOut = null;
                                if (fs.exists(fsStatePath)) {
//                            System.out.println("FS State APPENDING");
                                    fsStateOut = fs.append(fsStatePath);
                                } else {
//                            System.out.println("FS State CREATING");
                                    fsStateOut = fs.create(fsStatePath);
                                }
                                fsStateOut.write(fsState.getBytes());
                                // Newline
                                fsStateOut.write("\n".getBytes());
                                fsStateOut.close();
                            } else {
                                System.out.println(">> Filesystem State: ");
                                System.out.println(fsState);
                            }
                            break;
                    }


                }
            } catch (Throwable e) {
                e.printStackTrace();
            }

        }
        // Get Namenode Info
        // Open File for Append.
        if (fs instanceof DistributedFileSystem) {
            System.out.println("Filesystem Reference is Distributed");
        } else {
            System.out.println("Filesystem Reference is NOT distributed");
        }

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