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

package com.streever.hadoop.hdfs.shell.command;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.hadoop.shell.command.CommandReturn;

import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.hadoop.shell.Environment;

/**
 * Created by streever on 2015-11-22.
 */

public class LocalRm extends HdfsCommand {
    private boolean local = false;

    public LocalRm(String name, Environment env) {
        super(name, env);
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
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
