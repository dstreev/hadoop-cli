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

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;

import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;

public class HdfsConnect extends AbstractCommand {

    public static final String CURRENT_USER_PROP = "current.user";

    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "ozone-site.xml"};

    public HdfsConnect(String name) {
        super(name);
        Completer completer = new StringsCompleter("hdfs://localhost:8020/", "hdfs://hdfshost:8020/");
        this.completer = completer;
    }

    @Override
    public String getDescription() {
        return "Connect to HDFS";
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        try {
            if (env.getConfig() == null) {
                // Get a value that over rides the default, if nothing then use default.
                String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

                // Set a default
                if (hadoopConfDirProp == null)
                    hadoopConfDirProp = "/etc/hadoop/conf";

                Configuration config = new Configuration(true);

                File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
                for (String file : HADOOP_CONF_FILES) {
                    File f = new File(hadoopConfDir, file);
                    if (f.exists()) {
                        config.addResource(new Path(f.getAbsolutePath()));
                    }
                }
                // disable s3a fs cache
//                config.set("fs.s3a.impl.disable.cache", "true");
//                config.set("fs.s3a.bucket.probe","0");

                // hadoop.security.authentication
                if (config.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                    UserGroupInformation.setConfiguration(config);
                    env.getProperties().setProperty(CURRENT_USER_PROP, UserGroupInformation.getCurrentUser().getShortUserName());
//                log(env, UserGroupInformation.getCurrentUser().getUserName());
//                log(env, UserGroupInformation.getCurrentUser().getShortUserName());
                }

                Enumeration e = env.getProperties().propertyNames();

                while (e.hasMoreElements()) {
                    String key = (String) e.nextElement();
                    String value = env.getProperties().getProperty(key);
                    config.set(key, value);
                }

                FileSystem hdfs = null;
                try {
                    hdfs = FileSystem.get(config);
                } catch (Throwable t) {
                    t.printStackTrace();
                }

//            env.setValue(Constants.CFG, config);
                env.setConfig(config);
                FileSystemOrganizer fso = env.getFileSystemOrganizer();


//                FileSystemState fss = env.getCurrentFileSystemState();
//                fss.setWorkingDirectory(hdfs.makeQualified(new Path("/")));

                // set working dir to root
//            hdfs.setWorkingDirectory(hdfs.makeQualified(new Path("/")));

//                env.setRemoteWorkingDirectory(hdfs.makeQualified(new Path("/")));

//                FileSystem local = FileSystem.getLocal(new Configuration());
//                env.setLocalFileSystem(local);

//                env.getProperties().setProperty(Constants.HDFS_URL, hdfs.getUri().toString());

//                FSUtil.prompt(env);

//            if (!env.isSilent())
//                logv(env, "Connecting to default FS: " + hdfs.getUri());
//
//                logv(env, "HDFS CWD: " + hdfs.getWorkingDirectory());
//                logv(env, "HDFS CWD(env): " + env.getRemoteWorkingDirectory());
//                logv(env, "Local CWD: " + local.getWorkingDirectory());
            }
        } catch (IOException e) {
            log(env, e.getMessage());
        }
        return commandReturn;
    }

    @Override
    public Completer getCompleter() {
        return this.completer;
    }


}
