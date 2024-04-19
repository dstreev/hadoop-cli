/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.cli;

import com.cloudera.utils.hadoop.hdfs.shell.command.*;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import com.cloudera.utils.hadoop.hdfs.util.HdfsLsPlus;
import com.cloudera.utils.hadoop.hdfs.util.HdfsSource;
import com.cloudera.utils.hadoop.shell.commands.Env;
import com.cloudera.utils.hadoop.shell.commands.Exit;
import com.cloudera.utils.hadoop.shell.commands.Help;
import com.cloudera.utils.hadoop.shell.commands.HistoryCmd;
import com.cloudera.utils.hadoop.yarn.ContainerStatsCommand;
import com.cloudera.utils.hadoop.yarn.SchedulerStatsCommand;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static com.cloudera.utils.hadoop.hdfs.shell.command.HdfsConnect.*;

@Configuration
@Slf4j
@Getter
@Setter
public class HadoopCliAppCfg {

    @Bean
    @Order(100)
    @ConditionalOnProperty(
            name = "hadoop.cli.init")
    CommandLineRunner configInit(CliEnvironment cliEnvironment, @Value("${hadoop.cli.init}") String value) {
        return args -> {
            log.info("Setting Init: {}", value);
            try {
                cliEnvironment.runFile(value, null, null);
            } catch (DisabledException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.execute")
    CommandLineRunner configExecute(CliEnvironment cliEnvironment, @Value("${hadoop.cli.execute}") String value) {
        return args -> {
            log.info("Setting Execute: {}", value);
            try {
                cliEnvironment.processInput(value);
            } catch (DisabledException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.template")
    CommandLineRunner configTemplate(CliEnvironment cliEnvironment, @Value("${hadoop.cli.template}") String value) {
        return args -> {
            log.info("Setting Template: {}", value);
            cliEnvironment.setTemplate(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.template-delimiter")
    CommandLineRunner configTemplateDelimiter(CliEnvironment cliEnvironment, @Value("${hadoop.cli.template-delimiter}") String value) {
        return args -> {
            log.info("Setting Template Delimiter: {}", value);
            cliEnvironment.setTemplateDelimiter(value);
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.stdin")
    CommandLineRunner configStdIn(CliEnvironment cliEnvironment, @Value("${hadoop.cli.stdin}") String value) {
        return args -> {
            log.info("Setting StdIn: {}", value);
            // TODO: Work out how to handle StdIn
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.silent")
    CommandLineRunner configSilent(CliEnvironment cliEnvironment, @Value("${hadoop.cli.silent}") String value) {
        return args -> {
            log.info("Setting Silent: {}", value);
            cliEnvironment.setSilent(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.api")
    CommandLineRunner configApiMode(CliEnvironment cliEnvironment, @Value("${hadoop.cli.api}") String value) {
        return args -> {
            log.info("Setting ApiMode: {}", value);
            cliEnvironment.setApiMode(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.disabled")
    CommandLineRunner configDisabled(CliEnvironment cliEnvironment, @Value("${hadoop.cli.disabled}") String value) {
        return args -> {
            log.info("Setting Disabled: {}", value);
            cliEnvironment.setDisabled(Boolean.parseBoolean(value));
        };
    }


    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.verbose")
    CommandLineRunner configVerbose(CliEnvironment cliEnvironment, @Value("${hadoop.cli.verbose}") String value) {
        return args -> {
            log.info("Setting Verbose: {}", value);
            cliEnvironment.setVerbose(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.debug")
    CommandLineRunner configDebug(CliEnvironment cliEnvironment, @Value("${hadoop.cli.debug}") String value) {
        return args -> {
            log.info("Setting debug: {}", value);
            cliEnvironment.setDebug(Boolean.parseBoolean(value));
        };
    }

    @Bean
    @Order(50)
    @ConditionalOnProperty(
            name = "hadoop.cli.env-file")
    CommandLineRunner configEnvFile(CliEnvironment cliEnvironment, @Value("${hadoop.cli.env-file}") String value) {
        return args -> {
            log.info("Setting Env-File: {}", value);
//            String envProps = cmd.getOptionValue(value);
            File envPropsFile = new File(value);
            try {
                // Resolve SymLink if necessary
                Path envPropsPath = envPropsFile.toPath();
                if (Files.isSymbolicLink(envPropsFile.toPath())) {
                    // Reset path to look for 'real' file.
                    envPropsPath = envPropsPath.toRealPath();
                }
                InputStream inProps = Files.newInputStream(envPropsPath);
                Properties extProps = new Properties();
                extProps.load(inProps);
                cliEnvironment.getProperties().putAll(extProps);
            } catch (FileNotFoundException e) {
                System.out.println("Couldn't locate 'env-file' " + envPropsFile);
                System.out.println("Additional environment vars NOT loaded");
            } catch (IOException e) {
                System.out.println("Problems reading 'env-file' " + envPropsFile + " - " + e.getMessage());
                System.out.println("Additional environment vars NOT loaded");
            }
        };
    }

    @Bean
    @Order(1)
    @ConditionalOnProperty(
            name = "hadoop.cli.help")
    CommandLineRunner configHelp(CliEnvironment cliEnvironment, @Value("${hadoop.cli.help}") String value) {
        return args -> {
            log.info("Help: " + value);
            // TODO: Implement Help
        };
    }

    @Bean
    @Order(10)
    CommandLineRunner initHadoopConfiguration(CliEnvironment cliEnvironment) {
        return args -> {
            // TODO: Need to see if this attempts a connection to HDFS.
            if (!cliEnvironment.isDisabled()) {
                try {
                    // Get a value that over rides the default, if nothing then use default.
                    String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

                    org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration(true);
                    cliEnvironment.setHadoopConfig(config);

                    File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
                    for (String file : HADOOP_CONF_FILES) {
                        File f = new File(hadoopConfDir, file);
                        if (f.exists()) {
                            config.addResource(new org.apache.hadoop.fs.Path(f.getAbsolutePath()));
                        }
                    }
                    // disable s3a fs cache
//                config.set("fs.s3a.impl.disable.cache", "true");
//                config.set("fs.s3a.bucket.probe","0");

                    // hadoop.security.authentication
                    if (config.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                        UserGroupInformation.setConfiguration(config);
                        cliEnvironment.getProperties().setProperty(CURRENT_USER_PROP, UserGroupInformation.getCurrentUser().getShortUserName());
                    }

                    cliEnvironment.getProperties().stringPropertyNames().forEach(k -> {
                        config.set(k, cliEnvironment.getProperties().getProperty(k));
                    });

//                this.fileSystemOrganizer = fileSystemOrganizer;
//                    fileSystemOrganizer.init(config);

                    FileSystem hdfs = null;
                    try {
                        hdfs = FileSystem.get(config);
                    } catch (Throwable t) {
                        log.error("Error connecting to HDFS: {}", t.getMessage());
                    }

                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
        };
    }

    @Bean
    @Order(5)
    CommandLineRunner initCommands(CliEnvironment cliEnvironment) {
        return args -> {
//            setBannerResource("/hadoop_banner_0.txt");

            cliEnvironment.addCommand(new HdfsCd("cd", cliEnvironment));

            cliEnvironment.addCommand(new HdfsCd("cd", cliEnvironment));
            cliEnvironment.addCommand(new HdfsPwd("pwd"));

            // remote local
            cliEnvironment.addCommand(new HdfsCommand("get", cliEnvironment, Direction.REMOTE_LOCAL));
            cliEnvironment.addCommand(new HdfsCommand("copyFromLocal", cliEnvironment, Direction.LOCAL_REMOTE));
            // local remote
            cliEnvironment.addCommand(new HdfsCommand("put", cliEnvironment, Direction.LOCAL_REMOTE));
            cliEnvironment.addCommand(new HdfsCommand("copyToLocal", cliEnvironment, Direction.REMOTE_LOCAL));
            // src dest
            cliEnvironment.addCommand(new HdfsCommand("cp", cliEnvironment, Direction.REMOTE_REMOTE));

            // amend to context path, if present
            cliEnvironment.addCommand(new HdfsCommand("chown", cliEnvironment, Direction.NONE, 1));
            cliEnvironment.addCommand(new HdfsCommand("chmod", cliEnvironment, Direction.NONE, 1));
            cliEnvironment.addCommand(new HdfsCommand("chgrp", cliEnvironment, Direction.NONE, 1));

            cliEnvironment.addCommand(new HdfsAllowSnapshot("allowSnapshot", cliEnvironment, Direction.NONE, 1, false, true));
            cliEnvironment.addCommand(new HdfsDisallowSnapshot("disallowSnapshot", cliEnvironment, Direction.NONE, 1, false, true));
            cliEnvironment.addCommand(new HdfsLsSnapshottableDir("lsSnapshottableDir", cliEnvironment, Direction.NONE, 1, false, true));

            cliEnvironment.addCommand(new HdfsCommand("createSnapshot", cliEnvironment));
            cliEnvironment.addCommand(new HdfsCommand("deleteSnapshot", cliEnvironment));
            cliEnvironment.addCommand(new HdfsCommand("renameSnapshot", cliEnvironment));
            cliEnvironment.addCommand(new SnapshotDiff("snapshotDiff", cliEnvironment));

            cliEnvironment.addCommand(new HdfsCommand("du", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("df", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("dus", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("ls", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("lsr", cliEnvironment, Direction.NONE));
//        env.addCommand(new HdfsCommand("find", env, Direction.NONE, 1, false));

            cliEnvironment.addCommand(new HdfsCommand("mkdir", cliEnvironment, Direction.NONE));

            cliEnvironment.addCommand(new HdfsCommand("count", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("stat", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("tail", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("head", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("touchz", cliEnvironment, Direction.NONE));

            cliEnvironment.addCommand(new HdfsCommand("rm", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("rmdir", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("mv", cliEnvironment, Direction.REMOTE_REMOTE));
            cliEnvironment.addCommand(new HdfsCommand("cat", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("test", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("text", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("touchz", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("checksum", cliEnvironment, Direction.NONE));

//        addCommand(new HdfsScan("scan", cliEnvironment));

//        addCommand(new HdfsCommand("usage", cliEnvironment));

            // Security Help
//        env.addCommand(new HdfsUGI("ugi"));
//        env.addCommand(new HdfsKrb("krb", env, Direction.NONE, 1));

            // HDFS Utils
            //env.addCommand(new HdfsRepair("repair", env, Direction.NONE, 2, true, true));

            cliEnvironment.addCommand(new Env("env"));
            cliEnvironment.addCommand(new HdfsConnect("connect"));
            cliEnvironment.addCommand(new Help("help", cliEnvironment));
            cliEnvironment.addCommand(new HistoryCmd("history"));

            // HDFS Tools
            cliEnvironment.addCommand(new HdfsLsPlus("lsp", cliEnvironment, Direction.NONE));
//        addCommand(new HdfsNNStats("nnstat", cliEnvironment, Direction.NONE));

            cliEnvironment.addCommand(new HdfsSource("source", cliEnvironment));

            // MapReduce Tools
            // TODO: Add back once the field mappings are completed.
//        addCommand(new JhsStats("jhsstat", cliEnvironment, Direction.NONE));

            // Yarn Tools
            cliEnvironment.addCommand(new ContainerStatsCommand("cstat", cliEnvironment, Direction.NONE));
            cliEnvironment.addCommand(new SchedulerStatsCommand("sstat", cliEnvironment, Direction.NONE));

            cliEnvironment.addCommand(new Exit("exit"));
            cliEnvironment.addCommand(new LocalLs("lls", cliEnvironment));
            cliEnvironment.addCommand(new LocalPwd("lpwd"));
            cliEnvironment.addCommand(new LocalCd("lcd", cliEnvironment));

            cliEnvironment.addCommand(new LocalHead("lhead", cliEnvironment));
            cliEnvironment.addCommand(new LocalCat("lcat", cliEnvironment));
            cliEnvironment.addCommand(new LocalMkdir("lmkdir", cliEnvironment));
            cliEnvironment.addCommand(new LocalRm("lrm", cliEnvironment));

            cliEnvironment.addCommand(new Use("use", cliEnvironment));
            cliEnvironment.addCommand(new com.cloudera.utils.hadoop.hdfs.shell.command.List("list", cliEnvironment));
            cliEnvironment.addCommand(new com.cloudera.utils.hadoop.hdfs.shell.command.List("nss", cliEnvironment));
            cliEnvironment.addCommand(new List("namespaces", cliEnvironment));
        };
    }

    @Bean
    @Order(100)
    CommandLineRunner runInteractiveShell(CliEnvironment cliEnvironment, Shell shell) {
        return args -> {
            if (!cliEnvironment.isApiMode()) {
                log.info("Launching Interactive Shell");
                try {
                    shell.startShell(cliEnvironment);
                } catch (DisabledException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

}
