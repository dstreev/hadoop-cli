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
package com.cloudera.utils.hadoop.hdfs.shell.command;

import java.io.IOException;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalCd extends HdfsCommand {
//    private Environment env;

    public LocalCd(String name, Environment env) {
        super(name,env);
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        try {

            FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
            FileSystem localfs = env.getFileSystemOrganizer().getLocalFileSystem();//(FileSystem) env.getValue(Constants.LOCAL_FS);
            String dir = cmd.getArgs().length == 0 ? System
                            .getProperty("user.home") : cmd.getArgs()[0];
            AbstractCommand.logv(env, "Change Dir to: " + dir);
            AbstractCommand.logv(env, "CWD: " + lfss.getWorkingDirectory());

            Path newPath = null;

            if (dir.startsWith("~")) {
                dir = System.getProperty("user.home") + (dir.substring(1).length() > 1?dir.substring(1):"");
                newPath = new Path(dir);
            } else if (dir.startsWith("/")) {
                newPath = new Path(dir);
            } else {
                newPath = new Path(lfss.getWorkingDirectory(), dir);
            }

            FileStatus fstat = lfss.getFileSystem().getFileStatus(newPath);
            if (localfs.exists(newPath)) {
                AbstractCommand.logv(env, "exists");
                if (fstat.isDirectory()) {
                    lfss.setWorkingDirectory(newPath);
                } else {
                    AbstractCommand.logv(env, "Is not a directory: " + dir);
                }
            }

//            FSUtil.prompt(env);
        }
        catch (IOException e) {
            AbstractCommand.log(env, e.getMessage());
            commandReturn.setCode(AbstractCommand.CODE_LOCAL_FS_ISSUE);
            commandReturn.getErr().print(e.getMessage());
        }
        return commandReturn;
    }


}