package com.dstreev.hadoop.hdfs.util;

import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.instanceone.hdfs.shell.command.HdfsAbstract;
import com.instanceone.stemshell.Environment;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Created by dstreev on 2015-11-22.
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
    public void execute(Environment environment, CommandLine commandLine, ConsoleReader consoleReader) {
        System.out.println("Not implemented yet... :( ");
        /*

        Run fsck from the current hdfs path.

        Issue the repairs as they come in.  We don't want to get the whole list and then repair.
        Question: Will these be blocking?  Probably not, so we need another thread to work from a queue of entries
            that's populated by this process.

        From the OutputStream, get the files listed as "Under replicated"

        Split the line on ":" and issue an HDFS setrep to 3 on the file.


         */

    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("n", true, "repair file count limit");
        opts.addOption("r", true, "override replication value");
        return opts;
    }

}
