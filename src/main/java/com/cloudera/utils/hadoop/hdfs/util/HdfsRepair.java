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

import com.cloudera.utils.hadoop.hdfs.shell.command.Direction;
import com.cloudera.utils.hadoop.hdfs.shell.command.HdfsAbstract;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
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

    @Override
    public String getDescription() {
        return "HDFS Repair - Not Implemented Yet.";
    }

    public HdfsRepair(String name, CliEnvironment env, Direction directionContext ) {
        super(name, env, directionContext);
    }

    public HdfsRepair(String name, CliEnvironment env, Direction directionContext, int directives ) {
        super(name,env,directionContext,directives);
    }

    public HdfsRepair(String name, CliEnvironment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name,env,directionContext,directives,directivesBefore,directivesOptional);
    }

    public HdfsRepair(String name, CliEnvironment env) {
        super(name,env);
    }

    @Override
    public CommandReturn implementation(CliEnvironment cliEnvironment, CommandLine commandLine, CommandReturn commandReturn) {
        System.out.println("Not implemented yet... :( ");
        /*

        Run fsck from the current hdfs path.

        Issue the repairs as they come in.  We don't want to get the whole list and then repair.
        Question: Will these be blocking?  Probably not, so we need another thread to work from a queue of entries
            that's populated by this process.

        From the OutputStream, get the files listed as "Under replicated"

        Split the line on ":" and issue an HDFS setrep to 3 on the file.


         */
        commandReturn.setCode(-1);
        commandReturn.getErr().print("Repair not yet implemented");
        return commandReturn;
//         return new CommandReturn(-1, "repair not implemented yet");
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("n", true, "repair file count limit");
        opts.addOption("r", true, "override replication value");
        return opts;
    }

}
