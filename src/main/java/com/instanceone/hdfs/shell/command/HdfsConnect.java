// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.io.File;
import java.io.IOException;

import com.dstreev.hadoop.hdfs.shell.command.Constants;
import com.instanceone.stemshell.command.AbstractCommand;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.stemshell.Environment;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsConnect extends AbstractCommand {

    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

    public HdfsConnect(String name) {
        super(name);
        Completer completer = new StringsCompleter("hdfs://localhost:8020/", "hdfs://hdfshost:8020/");
        this.completer = completer;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            // Get a value that over rides the default, if nothing then use default.
// Requires Java 1.8...
//            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

            String hadoopConfDirProp = System.getenv().get(HADOOP_CONF_DIR);
            // Set a default
            if (hadoopConfDirProp == null)
                hadoopConfDirProp = "/etc/hadoop/conf";

            Configuration config = new Configuration(false);

            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
            for (String file : HADOOP_CONF_FILES) {
                File f = new File(hadoopConfDir, file);
                if (f.exists()) {
                    config.addResource(new Path(f.getAbsolutePath()));
                }
            }

            // hadoop.security.authentication
            if (config.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                UserGroupInformation.setConfiguration(config);
            }

            FileSystem hdfs = null;
            try {
                hdfs = FileSystem.get(config);
            } catch (Throwable t) {
                t.printStackTrace();
            }

            env.setValue(Constants.CFG, config);
            env.setValue(Constants.HDFS, hdfs);
            // set working dir to root
            hdfs.setWorkingDirectory(hdfs.makeQualified(new Path("/")));

            FileSystem local = FileSystem.getLocal(new Configuration());
            env.setValue(Constants.LOCAL_FS, local);
            env.setProperty(Constants.HDFS_URL, hdfs.getUri().toString());

            FSUtil.prompt(env);

            log(cmd, "Connected: " + hdfs.getUri());
            logv(cmd, "HDFS CWD: " + hdfs.getWorkingDirectory());
            logv(cmd, "Local CWD: " + local.getWorkingDirectory());

        } catch (IOException e) {
            log(cmd, e.getMessage());
        }
    }

    @Override
    public Completer getCompleter() {
        return this.completer;
    }


}
