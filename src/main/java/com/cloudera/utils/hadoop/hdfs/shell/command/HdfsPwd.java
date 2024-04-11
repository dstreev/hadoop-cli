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

package com.cloudera.utils.hadoop.hdfs.shell.command;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class HdfsPwd extends HdfsCommand {

    public HdfsPwd(String name) {
        super(name);
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {
//        FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);
        FileSystemState fss = env.getFileSystemOrganizer().getCurrentFileSystemState();
        String wd = fss.getWorkingDirectory().toString();
//        String wd = hdfs.getWorkingDirectory().toString();
        if (!cmd.hasOption("l")) {
            log(env, wd);
        }
        else {
            log(env, fss.getURI() + wd);
            //.substring(env.getProperties().getProperty(Constants.HDFS_URL).length()));
        }
//        FSUtil.prompt(env);
        return commandReturn;
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("l", false, "show the full HDFS URL");
        return opts;
    }

}
