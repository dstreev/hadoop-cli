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

package com.streever.hadoop.hdfs.util;

import com.streever.hadoop.hdfs.replication.Definition;
import com.streever.hadoop.hdfs.replication.NamenodeHost;
import com.streever.hadoop.hdfs.replication.ReplicationHelper;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * The idea behind the Replication Manager is to coordinate the replication
 * of a directory between two clusters.
 * <p>
 * Using snapshots and snapshot diff reports, the replication manager will
 * use distcp to drive the replication between the two clusters.
 * <p>
 * For the first 'sync', a snapshot of the source directory is taken and
 * your basic distcp is used to sync that snapshot to the target cluster and
 * directory.
 * <p>
 * On subsequent replication jobs, another snapshot is taken and then a snapshot
 * diff report is generated between the last successful sync and the current snapshot.
 * This snapshot diff report is used to drive the activities of distcp.
 * <p>
 *
 * <p>
 * 'definition.json' - Job Definition
 * 'state.json' - Job State
 * <p>
 * Created by David Streever on 2016-04-05.
 */
public class ReplicationManager {
    /*
    Process:
        - Check the Hadoop Version to ensure the source cluster is at least 2.7.1, which provides support to drive distcp from a snapshot diff report.
        - Start application with HDFS location of the 'definition.json' file which defines the job.  The definition file will contain all the information required for the sync and doesn't need to be collocated with the target sync directory.
        - When doing replication between two HA clusters, additional dfs.nameservices are required.  You can either feed in a 'config' file that has this information in it and the application will use that to run the job, or you can use the default '/etc/hadoop/conf/hdfs|core-site.xml' files and 'augment' them with the additional dfs.nameservice information with information from the 'replication.json'.
        - Review (if exists) the .replication/<name>_state.json file.
        - If it doesn't exist, then we'll assume this is the first iteration and that we're initializing the target cluster with the full directory contents.  If the directory exists on the target cluster, we should NOT overwrite it.  We should create a new directory, as a sibling, and give it the name '<TARGET_DIR>.repl.<x>'.  Where the <x> represents previous version created to avoid overwriting.  If this scenario happens, the user needs to manually adjust the target directory name to replace the actual desired location AND update the replication.json file to ensure we are targeting the correct directory in subsequent sync jobs.

    QUESTIONS:
        - Should we take a few snapshots on the target directory and run diffs to ensure the contents haven't changed since the last sync.  This target directory should be READ-ONLY.
    ASSUMPTIONS:
         - Any hive compaction processes on the target cluster should be suspended, because it will mess up the replication state.
         - This should NOT be run as a means to sync HBase.  That should be done through the native HBase replication process.

     */

    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "ozone-site.xml"};

    private static final String PREFIX_HA_NAMENODES = "dfs.ha.namenodes";
    private static final String PREFIX_HA_NAMENODE_RPC_ADDRESS = "dfs.namenode.rpc-address";
    private static final String[] TARGET_NN_ALIASES = {"tnn1", "tnn2"};
    private static final String PREFIX_HA_NAMENODE_SERVICE_RPC_ADDRESS = "dfs.namenode.servicerpc-address";
    private static final String PREFIX_HA_NAMENODE_HTTP_ADDRESS = "dfs.namenode.http-address";
    private static final String PREFIX_HA_NAMENODE_HTTPS_ADDRESS = "dfs.namenode.https-address";

//    private static final String REPL_INFO_PATH = ".replication";
//    private static final String JOB_DEFINITION_EXT = "_def.json";
//    private static final String JOB_STATE_EXT = "_state.json";

    public ReplicationManager(String[] arguments) {
        // Handle the commandline options.
        Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, arguments);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Replication Manager", options);
            return;
        }

        // Get a handle to HDFS and serialize the configuraiton file.
        String hadoopConfDirProp = System.getenv().get(HADOOP_CONF_DIR);

        // Set a default
        if (hadoopConfDirProp == null)
            hadoopConfDirProp = "/etc/hadoop/conf";

        Configuration config = new Configuration(false);

        File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
        for (String file : HADOOP_CONF_FILES) {
            File f = new File(hadoopConfDir, file);
            if (f.exists()) {
                config.addResource(new Path(f.getAbsolutePath()));
            }
        }


        String defPathStr = cmd.getOptionValue("file");

        Path defPath = new Path(defPathStr);
        try {
            FileSystem hdfs = null;
            try {
                hdfs = FileSystem.get(config);
            } catch (Throwable t) {
                t.printStackTrace();
            }

            if (!(hdfs instanceof DistributedFileSystem)) {
                System.out.println("Source needs to be a Distributed File System.");
                return;
            }

            if (!hdfs.exists(defPath)) {
                System.out.println("Can't location definition file: " + defPathStr);
                return;
            }

            boolean initialLoad = false;
            String snapshotWatermark = null;
            String snapshotUpto = null;

            // Open the Definitions File and deserialize.
            FSDataInputStream defFSIn = hdfs.open(defPath);
            InputStream defIn = defFSIn.getWrappedStream();

            Definition definition = ReplicationHelper.definitionFromInputStream(defIn);

            System.out.println(definition.toString());

            defIn.close();

//                State state = null;
//
//                // Need to look for a state file.
//                if (hdfs.exists(replicationStatePath)) {
//                    FSDataInputStream stateIn = hdfs.open(replicationStatePath);
//                    InputStream stateIn = stateIn.getWrappedStream();
//                    state = ReplicationHelper.stateFromInputStream(stateIn);
//                } else {
//                    initialLoad = true;
//                }

            config = enhanceConfig(config, definition);

            // *** Take a snapshot. ***
            // Get the PathData element for the Source.
            String targetSourceSyncDirectory = definition.getSource().getDirectory();
            Path targetPath = new Path(targetSourceSyncDirectory);
            if (!hdfs.exists(targetPath) || !hdfs.isDirectory(targetPath)) {
                System.out.println("The target sync directory specified: " + targetSourceSyncDirectory + " either doesn't exist, you don't have permissions or it's not a directory");
                return;
            }

            SnapshottableDirectoryStatus[] snapshottableDirectoryStatuses = ((DistributedFileSystem) hdfs).getSnapshottableDirListing();
            SnapshottableDirectoryStatus targetStatus = null;
            for (SnapshottableDirectoryStatus status: snapshottableDirectoryStatuses) {
                if (targetPath.equals(status.getFullPath()))
                    targetStatus = status;
                System.out.println(status.getFullPath().toString());
            }

            if (targetStatus == null) {
                System.out.println("Directory: " + targetSourceSyncDirectory + " doesn't support 'snapshots'.");
            }


            // Take a snapshot of the target path.
            // Check for a snapshot name pattern.
            boolean snapshotNameUseDefault = true;
            String snapshotName = null;
            if (!definition.getSnapshotNamePattern().getUseDefault()) {
                snapshotNameUseDefault = false;
                DateFormat df = new SimpleDateFormat(definition.getSnapshotNamePattern().getNamePattern());
                snapshotName = df.format(new Date());
            }

            System.out.println("Snapshot Name: " + snapshotName);

            // Need to take a snapshot.
            Path snapshotPath = null;
            if (snapshotNameUseDefault) {
                snapshotPath = hdfs.createSnapshot(targetPath);
            } else {
                snapshotPath = hdfs.createSnapshot(targetPath, snapshotName);
            }

            System.out.println("Snapshot taken: " + snapshotPath.toString());
            String[] pathElements = snapshotPath.toString().split("/");
            String snapshotTrueName = pathElements[snapshotPath.depth()];
            System.out.println("Snapshot name: " + snapshotTrueName);

            // Build distcp Arguments.
            String[] argv = new String[7]; // -update -delete -diff <s1> <s2> <source> <target>
            argv[0] = "-update";
            argv[1] = "-delete";
            argv[2] = "-diff";
            // TODO: Get the right Snapshot Names.
            argv[3] = "ss1";
            argv[4] = "ss2";
            argv[5] = ReplicationHelper.getSourceDirectory(definition, config); // Need protocol leading source directory
            argv[6] = ReplicationHelper.getTargetDirectory(definition); // Need protocol leading target directory

            // TODO: Need to check that the target is "snapshottable".
            // TODO: Need to check that the target has a snapshot matching the "s1" snapshot name.

//            snapshotPath.depth()
            // Close the current file system.
            hdfs.close();

            int exitCode;
            try {
//                DistCp e = new DistCp();
//                DistCp.Cleanup CLEANUP = new DistCp.Cleanup(e);
//                ShutdownHookManager.get().addShutdownHook(CLEANUP, 30);
//                exitCode = ToolRunner.run(config, e, argv);
            } catch (Exception var4) {
//                LOG.error("Couldn\'t complete DistCp operation: ", var4);
                exitCode = -999;
            }

            // TODO: On successful completion, create a snapshot on the target that matches the snapshot "to" from the source.
            FileSystem targetHdfs = null;
            try {
                URI targetURI = new URI("hdfs://" + definition.getTarget().getName());
                targetHdfs = FileSystem.get(targetURI, config);
            } catch (Throwable t) {
                t.printStackTrace();
            }

            // Once this is complete, we need to create a snaphot on the target directory that matches the
            // "to" snapshot used in the source.

//            System.exit(exitCode);

            // TODO: Snapshot Maintenance.  Flag to specify that we'll cleanup old snapshots.



        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    /**
     * Determine if we need to supplement the existing configuration with the
     * dfs service information in the definitions file.  If we do, we'll need to
     * construct a new config, using the current one as a basis.
     *
     * @param config
     * @param definition
     * @return
     */
    private Configuration enhanceConfig(Configuration config, Definition definition) {

        Configuration rtn = new Configuration(config);

        // This new config will be necessary when supplying distcp the two endpoints in the transfer.
        if (definition.getTarget().getHaEnabled() && !definition.getTarget().getDfsNameService().getDefined()) {
            // Need to Supplement.

            // Rebuild the config. to include the target HA configurations.
            Boolean currentClusterIsHA = rtn.getBoolean("dfs.ha.automatic-failover.enabled", false);
            System.out.println("Current Cluster is HA: " + currentClusterIsHA);
            if (currentClusterIsHA) {
                String currentNameService = rtn.get("dfs.nameservices").split(",")[0];
                System.out.println("Current Nameservice: " + currentNameService);
                String targetDfsNameService = definition.getTarget().getName();
                rtn.set("dfs.nameservices", currentNameService + "," + targetDfsNameService);
                System.out.println("New NameServices: " + config.get("dfs.nameservices"));
                // Set aliases
                rtn.set(PREFIX_HA_NAMENODES + "." + targetDfsNameService, TARGET_NN_ALIASES[0] + "," + TARGET_NN_ALIASES[1]);
                // Set Alias hosts.
                int i = 0;
                for (NamenodeHost nnh : definition.getTarget().getDfsNameService().getNamenodeHosts()) {
                    String hostNRpcPort = nnh.getFqdn() + ":" + nnh.getRpcPort();
                    String hostNServiceRpcPort = nnh.getFqdn() + ":" + nnh.getServiceRpcPort();
                    String hostNHttpPort = nnh.getFqdn() + ":" + nnh.getHttpPort();
                    String hostNHttpsPort = nnh.getFqdn() + ":" + nnh.getHttpsPort();
                    rtn.set(PREFIX_HA_NAMENODE_RPC_ADDRESS + "." + targetDfsNameService + "." + TARGET_NN_ALIASES[0], hostNRpcPort);
                    rtn.set(PREFIX_HA_NAMENODE_SERVICE_RPC_ADDRESS + "." + targetDfsNameService + "." + TARGET_NN_ALIASES[0], hostNServiceRpcPort);
                    rtn.set(PREFIX_HA_NAMENODE_HTTP_ADDRESS + "." + targetDfsNameService + "." + TARGET_NN_ALIASES[0], hostNHttpPort);
                    rtn.set(PREFIX_HA_NAMENODE_HTTPS_ADDRESS + "." + targetDfsNameService + "." + TARGET_NN_ALIASES[0], hostNHttpsPort);
                }
            } else {
                System.out.println("Not Yet Supported: Current HA and Target NOT.");
            }

        }
        return rtn;
    }

    public Options getOptions() {
        Options options = new Options();
        options.addOption("v", "verbose", false, "show verbose output");

        Option replDefOption = new Option("f", "file", true, "Replication Definition File");
        replDefOption.setRequired(true);
//        Option replDefOption = Option.builder("f").required(true)
//                .argName("file")
//                .desc("Replication Definition File")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .longOpt("file")
//                .build();
        options.addOption(replDefOption);

//        Option cfgOption = Option.builder("cfg").required(false)
//                .argName("config")
//                .desc("Non Default(/etc/hadoop/conf) Hadoop Configuration Directory")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .longOpt("config")
//                .build();
//        options.addOption(cfgOption);

//        Option nameOption = Option.builder("n").required(false)
//                .argName("name")
//                .desc("Definition Name")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .longOpt("name")
//                .build();
//        options.addOption(nameOption);

        Option helpOption = new Option("h", "help", false, "Help");
        helpOption.setRequired(false);
        //        Option helpOption = Option.builder("h").required(false)
        //                .argName("help")
        //                .desc("Help")
        //                .hasArg(false)
        //                .longOpt("help")
        //                .build();
        options.addOption(helpOption);

        return options;
    }

}
