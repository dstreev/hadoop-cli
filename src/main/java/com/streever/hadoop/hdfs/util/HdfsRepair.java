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

import com.streever.hadoop.hdfs.shell.command.Direction;
import com.streever.hadoop.hdfs.shell.command.HdfsAbstract;
import com.streever.tools.stemshell.Environment;
import com.streever.tools.stemshell.command.CommandReturn;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Created by streever on 2015-11-22.
 *
 * Syntax:
 *  repair [-n <count>] [path]
 *  repair -n 100 /user
 */
public class HdfsRepair extends HdfsAbstract {

    public HdfsRepair(String name) {
        super(name);
    }

    public HdfsRepair(String name, Environment env, Direction directionContext ) {
        super(name, env, directionContext);
    }

    public HdfsRepair(String name, Environment env, Direction directionContext, int directives ) {
        super(name,env,directionContext,directives);
    }

    public HdfsRepair(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name,env,directionContext,directives,directivesBefore,directivesOptional);
    }

    public HdfsRepair(String name, Environment env) {
        super(name,env);
    }

    @Override
    public CommandReturn execute(Environment environment, CommandLine commandLine, ConsoleReader consoleReader) {
        System.out.println("Not implemented yet... :( ");
        /*

        Run fsck from the current hdfs path.

        Issue the repairs as they come in.  We don't want to get the whole list and then repair.
        Question: Will these be blocking?  Probably not, so we need another thread to work from a queue of entries
            that's populated by this process.

        From the OutputStream, get the files listed as "Under replicated"

        Split the line on ":" and issue an HDFS setrep to 3 on the file.


         */
         return new CommandReturn(-1, "repair not implemented yet");
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("n", true, "repair file count limit");
        opts.addOption("r", true, "override replication value");
        return opts;
    }

}
