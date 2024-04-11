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

import static com.cloudera.utils.hadoop.hdfs.shell.command.FSUtil.longFormat;
import static com.cloudera.utils.hadoop.hdfs.shell.command.FSUtil.shortFormat;

import java.io.IOException;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalLs extends HdfsCommand {
//    private Environment env;

    public LocalLs(String name, CliEnvironment env) {
        super(name, env);
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {
        try {
            FileSystem localfs = env.getFileSystemOrganizer().getLocalFileSystem();
            FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);

            Path srcPath = cmd.getArgs().length == 0 ? lfss.getWorkingDirectory() : new Path(lfss.getWorkingDirectory(), cmd.getArgs()[0]);
            FileStatus[] files = localfs.listStatus(srcPath);
            for (FileStatus file : files) {
                if (cmd.hasOption("l")) {
                    log(env, longFormat(file));
                }
                else {
                    log(env, shortFormat(file));
                }
            }
//            FSUtil.prompt(env);
        }
        catch (IOException e) {
            log(env, e.getMessage());
        }
        return commandReturn;
    }


    @Override
    public Options getOptions() {
        // TODO Auto-generated method stub
        Options opts = super.getOptions();
        opts.addOption("l", false, "show extended file attributes");
        return opts;
    }  

}
