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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import org.apache.commons.cli.CommandLine;

public class HdfsConnect extends AbstractCommand {

    public static final String CURRENT_USER_PROP = "current.user";
    public static final String DEFAULT_FS = "fs.defaultFS";
    public static final String FS_USER_DIR = "dfs.user.home.dir.prefix";

    public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    public static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "ozone-site.xml"};

    public HdfsConnect(String name) {
        super(name);
        Completer completer = new StringsCompleter("hdfs://localhost:8020/", "hdfs://hdfshost:8020/");
        this.completer = completer;
    }

    @Override
    public String getDescription() {
        return "Connect to HDFS";
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {
    // Moved to Environment Bean.
        //        try {
//            if (env.getConfig() == null) {
//                // Get a value that over rides the default, if nothing then use default.
//                String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");
////                System.out.println("Hadoop Conf: " + hadoopConfDirProp);
//                // Set a default
//                if (hadoopConfDirProp == null)
//                    hadoopConfDirProp = "/etc/hadoop/conf";
//
//                Configuration config = new Configuration(true);
//
//                File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
//                for (String file : HADOOP_CONF_FILES) {
//                    File f = new File(hadoopConfDir, file);
//                    if (f.exists()) {
//                        config.addResource(new Path(f.getAbsolutePath()));
//                    }
//                }
//                // disable s3a fs cache
////                config.set("fs.s3a.impl.disable.cache", "true");
////                config.set("fs.s3a.bucket.probe","0");
//
//                // hadoop.security.authentication
//                if (config.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
//                    UserGroupInformation.setConfiguration(config);
//                    env.getProperties().setProperty(CURRENT_USER_PROP, UserGroupInformation.getCurrentUser().getShortUserName());
////                log(env, UserGroupInformation.getCurrentUser().getUserName());
////                log(env, UserGroupInformation.getCurrentUser().getShortUserName());
//                }
//
//                Enumeration e = env.getProperties().propertyNames();
//
//                while (e.hasMoreElements()) {
//                    String key = (String) e.nextElement();
//                    String value = env.getProperties().getProperty(key);
//                    config.set(key, value);
//                }
//
//                FileSystem hdfs = null;
//                try {
//                    hdfs = FileSystem.get(config);
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                }
//
////            env.setValue(Constants.CFG, config);
//                env.setConfig(config);
//                FileSystemOrganizer fso = env.getFileSystemOrganizer();
//
//
////                FileSystemState fss = env.getCurrentFileSystemState();
////                fss.setWorkingDirectory(hdfs.makeQualified(new Path("/")));
//
//                // set working dir to root
////            hdfs.setWorkingDirectory(hdfs.makeQualified(new Path("/")));
//
////                env.setRemoteWorkingDirectory(hdfs.makeQualified(new Path("/")));
//
////                FileSystem local = FileSystem.getLocal(new Configuration());
////                env.setLocalFileSystem(local);
//
////                env.getProperties().setProperty(Constants.HDFS_URL, hdfs.getUri().toString());
//
////                FSUtil.prompt(env);
//
////            if (!env.isSilent())
////                logv(env, "Connecting to default FS: " + hdfs.getUri());
////
////                logv(env, "HDFS CWD: " + hdfs.getWorkingDirectory());
////                logv(env, "HDFS CWD(env): " + env.getRemoteWorkingDirectory());
////                logv(env, "Local CWD: " + local.getWorkingDirectory());
//            }
//        } catch (IOException e) {
//            log(env, e.getMessage());
//        }
        return commandReturn;
    }


    @Override
    public Completer getCompleter() {
        return this.completer;
    }


}
