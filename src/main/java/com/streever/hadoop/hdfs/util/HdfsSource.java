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

import com.streever.hadoop.HadoopSession;
import com.streever.hadoop.hdfs.shell.command.Constants;
import com.streever.hadoop.hdfs.shell.command.HdfsAbstract;
import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.CommandReturn;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;

import java.io.IOException;
import java.net.URI;

public class HdfsSource  extends HdfsAbstract {

    private FileSystem fs = null;

    private HadoopSession shell;
    private Configuration configuration = null;
    private DFSClient dfsClient = null;

    public HdfsSource(String name, Environment env, HadoopSession shell) {
        super(name, env);
        this.shell = shell;
    }

    @Override
    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {

        logv(env, "Beginning 'source' collection.");

        // Get the Filesystem
        configuration = (Configuration) env.getValue(Constants.CFG);

        String hdfs_uri = (String) env.getProperties().getProperty(Constants.HDFS_URL);

        fs = (FileSystem) env.getValue(Constants.HDFS);

        if (fs == null) {
            log(env, "Please connect first");
            CommandReturn cr = new CommandReturn(CODE_NOT_CONNECTED);
            cr.getErr().print("Not connected. Connect first.");
            return cr;
        }

        URI nnURI = fs.getUri();

        try {
            dfsClient = new DFSClient(nnURI, configuration);
        } catch (IOException e) {
            e.printStackTrace();
            CommandReturn cr = new CommandReturn(CODE_CONNECTION_ISSUE);
            cr.getErr().print(e.getMessage());
            return cr;
        }

        Option[] cmdOpts = cmd.getOptions();
        String[] cmdArgs = cmd.getArgs();

        String template = null;
        String delimiter = null;
        if (cmd.hasOption("t")) {
            template = cmd.getOptionValue("t");
        }
        if (cmd.hasOption("d")) {
            delimiter = cmd.getOptionValue("d");
        }
        if (cmd.hasOption("lf")) {
            // TODO: FIX THIS
            runSource(cmd.getOptionValue("lf"), template, delimiter);
        }

        logv(env,"'Source' complete.");

        return commandReturn;
    }

    private void runSource(String sourceFile, String template, String delimiter) {
        this.shell.runFile(sourceFile,template, delimiter);
    }


    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        Option lfileOption = new Option("lf", "localfile", true, "local file to run");
        // For Commons-CLI v 1.3+
        //        Option lfileOption = Option.builder("lf").required(false)
        //               .argName("source local file")
        //                .desc("local file to run")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("localfile")
        //                .build();
        opts.addOption(lfileOption);

        Option templateOption = new Option("t", "template", true, "Message Template");
        //        Option templateOption = Option.builder("t").required(false)
        //                .argName("template")
        //                .desc("Message Template")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("template")
        //                .build();
        opts.addOption(templateOption);

        Option delimiterOption = new Option("d", "delimiter", true, "delimiter");
        //        Option delimiterOption = Option.builder("d").required(false)
        //                .argName("delimiter")
        //                .desc("delimiter")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("delimiter")
        //                .build();
        opts.addOption(delimiterOption);

        // TODO: Add Distributed File Source
//        Option dfileOption = Option.builder("df").required(false)
//                .argName("source distributed file")
//                .desc("distributed file to run")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .longOpt("distributedfile")
//                .build();
//        opts.addOption(dfileOption);

//
//        Option commentOption = Option.builder("c").required(false)
//                .argName("comment")
//                .desc("Add comment to output")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .longOpt("comment")
//                .build();
//        opts.addOption(commentOption);

        return opts;
    }

}
