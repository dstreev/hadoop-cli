// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

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
        if (cmd.hasOption("l") || !cmd.hasOption("s")) {
            Properties props = env.getProperties();
            log(cmd, "Local Properties:");
            for (Object key : props.keySet()) {
                log(cmd, "\t" + key + "=" + props.get(key));
            }
        }
        if (cmd.hasOption("s")) {
            log(cmd, "System Properties:");
            Properties props = System.getProperties();
            for (Object key : props.keySet()) {
                log(cmd, "\t" + key + "=" + props.get(key));
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
