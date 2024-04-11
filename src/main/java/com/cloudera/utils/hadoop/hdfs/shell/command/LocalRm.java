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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by streever on 2015-11-22.
 */

public class LocalRm extends HdfsCommand {
    private boolean local = false;

    public LocalRm(String name, CliEnvironment env) {
        super(name, env);
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {
        CommandReturn cr = commandReturn;
        try {
//            FileSystem hdfs = this.local ? (FileSystem) env.getValue(Constants.LOCAL_FS)
//                            : (FileSystem) env.getValue(Constants.HDFS);
            // TODO: Need to review this.  Looks like it's working partially on Remote FS and NOT Local.
            FileSystem lfs = getEnv().getFileSystemOrganizer().getLocalFileSystem();
            String remoteFile = cmd.getArgs()[0];

            logv(env, "HDFS file: " + remoteFile);
//            Path hdfsPath = new Path(hdfs.getWorkingDirectory(), remoteFile);
            Path hdfsPath = null;//new Path(env.getRemoteWorkingDirectory(), remoteFile);
            logv(env, "Remote path: " + hdfsPath);

            boolean recursive = cmd.hasOption("r");
            logv(env, "Deleting recursively...");
            lfs.delete(hdfsPath, recursive);

//            FSUtil.prompt(env);

        }
        catch (Throwable e) {
            cr.setCode(CODE_CMD_ERROR);
            cr.getErr().print(e.getMessage());
        }
        return cr;
    }

    @Override
    public String getDescription() {
        return "Local FS 'rm' command";
    }

    @Override
    public Options getOptions() {
        Options opts =  super.getOptions();
        opts.addOption("r", false, "delete recursively");
        return opts;
    }
}
