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

package com.cloudera.utils.hadoop.hdfs.util;

import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.hdfs.shell.command.HdfsAbstract;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
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

//    private HadoopSession shell;
    private Configuration configuration = null;
    private DFSClient dfsClient = null;

    public HdfsSource(String name, CliEnvironment env) {
        super(name, env);
//        this.shell = shell;
    }

    @Override
    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {

        AbstractCommand.logv(env, "Beginning 'source' collection.");

        // Get the Filesystem
        configuration = env.getHadoopConfig();

        fs = env.getFileSystemOrganizer().getCurrentFileSystemState().getFileSystem();
        //(FileSystem) env.getValue(Constants.HDFS);

        if (fs == null) {
            AbstractCommand.log(env, "Please connect first");
            CommandReturn cr = new CommandReturn(AbstractCommand.CODE_NOT_CONNECTED);
            cr.getErr().print("Not connected. Connect first.");
            return cr;
        }

        URI nnURI = fs.getUri();

        try {
            dfsClient = new DFSClient(nnURI, configuration);
        } catch (IOException e) {
            e.printStackTrace();
            CommandReturn cr = new CommandReturn(AbstractCommand.CODE_CONNECTION_ISSUE);
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
            try {
                runSource(cmd.getOptionValue("lf"), template, delimiter);
            } catch (DisabledException e) {
                throw new RuntimeException(e);
            }
        }

        AbstractCommand.logv(env,"'Source' complete.");

        return commandReturn;
    }

    private void runSource(String sourceFile, String template, String delimiter) throws DisabledException {
        env.runFile(sourceFile,template, delimiter);
    }


    @Override
    public String getDescription() {
        return "Run an external script of commands.";
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
