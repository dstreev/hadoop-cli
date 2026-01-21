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
import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.hdfs.shell.command.HdfsAbstract;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;

import java.io.IOException;
import java.net.URI;

@Slf4j
public class HdfsSource extends HdfsAbstract {

    public HdfsSource(String name) {
        super(name);
    }

    @Override
    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {

        AbstractCommand.logv(session, "Beginning 'source' collection.");

        // Get the Filesystem
        Configuration configuration = session.getHadoopConfig();

        FileSystem fs = session.getFileSystemOrganizer().getCurrentFileSystemState().getFileSystem();

        if (fs == null) {
            AbstractCommand.log(session, "Please connect first");
            CommandReturn cr = new CommandReturn(AbstractCommand.CODE_NOT_CONNECTED);
            cr.getErr().print("Not connected. Connect first.");
            return cr;
        }

        URI nnURI = fs.getUri();

        try {
            DFSClient dfsClient = new DFSClient(nnURI, configuration);
        } catch (IOException e) {
            log.error("Error connecting to HDFS: {} - {}", e.getMessage(), e.getCause(), e);
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
            try {
                runSource(session, cmd.getOptionValue("lf"), template, delimiter);
            } catch (DisabledException e) {
                throw new RuntimeException(e);
            }
        }

        AbstractCommand.logv(session,"'Source' complete.");

        return commandReturn;
    }

    private void runSource(CliSession session, String sourceFile, String template, String delimiter) throws DisabledException {
        session.runFile(sourceFile, template, delimiter);
    }


    @Override
    public String getDescription() {
        return "Run an external script of commands.";
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        Option lfileOption = new Option("lf", "localfile", true, "local file to run");
        opts.addOption(lfileOption);

        Option templateOption = new Option("t", "template", true, "Message Template");
        opts.addOption(templateOption);

        Option delimiterOption = new Option("d", "delimiter", true, "delimiter");
        opts.addOption(delimiterOption);

        return opts;
    }

}
