// Copyright (c) 2012 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.util.Properties;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public class Env extends AbstractCommand {

    public Env(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        if (cmd.hasOption("l")) {
            Properties props = env.getProperties();
            System.out.println("Local Properties:");
            for (Object key : props.keySet()) {
                System.out.println("\t" + key + "=" + props.get(key));
            }
        }
        if (cmd.hasOption("s")) {
            System.out.println("System Properties:");
            Properties props = System.getProperties();
            for (Object key : props.keySet()) {
                System.out.println("\t" + key + "=" + props.get(key));
            }
        }
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("s", "system", false, "list system properties.");
        opts.addOption("l", "local", false, "list local properties.");
        return opts;
    }

}
