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
            if (!cliEnvironment.isInitialized()) {
                cliEnvironment.init();
            }
        };
    }

    @Bean
    @Order(5)
    CommandLineRunner initCommands(CliEnvironment cliEnvironment) {
        return args -> {
//            setBannerResource("/hadoop_banner_0.txt");

            cliEnvironment.addCommand(new HdfsCd("cd"));
            cliEnvironment.addCommand(new HdfsPwd("pwd"));

            // remote local
            cliEnvironment.addCommand(new HdfsCommand("get", Direction.REMOTE_LOCAL));
            cliEnvironment.addCommand(new HdfsCommand("copyFromLocal", Direction.LOCAL_REMOTE));
            // local remote
            cliEnvironment.addCommand(new HdfsCommand("put", Direction.LOCAL_REMOTE));
            cliEnvironment.addCommand(new HdfsCommand("copyToLocal", Direction.REMOTE_LOCAL));
            // src dest
            cliEnvironment.addCommand(new HdfsCommand("cp", Direction.REMOTE_REMOTE));

            // amend to context path, if present
            cliEnvironment.addCommand(new HdfsCommand("chown", Direction.NONE, 1));
            cliEnvironment.addCommand(new HdfsCommand("chmod", Direction.NONE, 1));
            cliEnvironment.addCommand(new HdfsCommand("chgrp", Direction.NONE, 1));

            cliEnvironment.addCommand(new HdfsAllowSnapshot("allowSnapshot", Direction.NONE, 1, false, true));
            cliEnvironment.addCommand(new HdfsDisallowSnapshot("disallowSnapshot", Direction.NONE, 1, false, true));
            cliEnvironment.addCommand(new HdfsLsSnapshottableDir("lsSnapshottableDir", Direction.NONE, 1, false, true));

            cliEnvironment.addCommand(new HdfsCommand("createSnapshot"));
            cliEnvironment.addCommand(new HdfsCommand("deleteSnapshot"));
            cliEnvironment.addCommand(new HdfsCommand("renameSnapshot"));
            cliEnvironment.addCommand(new SnapshotDiff("snapshotDiff"));

            cliEnvironment.addCommand(new HdfsCommand("du", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("df", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("dus", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("ls", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("lsr", Direction.NONE));
//        env.addCommand(new HdfsCommand("find", env, Direction.NONE, 1, false));

            cliEnvironment.addCommand(new HdfsCommand("mkdir", Direction.NONE));

            cliEnvironment.addCommand(new HdfsCommand("count", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("stat", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("tail", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("head", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("touchz", Direction.NONE));

            cliEnvironment.addCommand(new HdfsCommand("rm", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("rmdir", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("mv", Direction.REMOTE_REMOTE));
            cliEnvironment.addCommand(new HdfsCommand("cat", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("test", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("text", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("touchz", Direction.NONE));
            cliEnvironment.addCommand(new HdfsCommand("checksum", Direction.NONE));

//        addCommand(new HdfsScan("scan", cliEnvironment));

//        addCommand(new HdfsCommand("usage", cliEnvironment));

            // Security Help
//        env.addCommand(new HdfsUGI("ugi"));
//        env.addCommand(new HdfsKrb("krb", env, Direction.NONE, 1));

            // HDFS Utils
            //env.addCommand(new HdfsRepair("repair", env, Direction.NONE, 2, true, true));

            cliEnvironment.addCommand(new Env("env"));
            cliEnvironment.addCommand(new HdfsConnect("connect"));
            cliEnvironment.addCommand(new Help("help", cliEnvironment.getDefaultRegistry()));
            cliEnvironment.addCommand(new HistoryCmd("history"));

            // HDFS Tools
            cliEnvironment.addCommand(new HdfsLsPlus("lsp", Direction.NONE));
//        addCommand(new HdfsNNStats("nnstat", cliEnvironment, Direction.NONE));

            cliEnvironment.addCommand(new HdfsSource("source"));

            // MapReduce Tools
            // TODO: Add back once the field mappings are completed.
//        addCommand(new JhsStats("jhsstat", cliEnvironment, Direction.NONE));

            // Yarn Tools
            cliEnvironment.addCommand(new ContainerStatsCommand("cstat", Direction.NONE));
            cliEnvironment.addCommand(new SchedulerStatsCommand("sstat", Direction.NONE));

            cliEnvironment.addCommand(new Exit("exit"));
            cliEnvironment.addCommand(new LocalLs("lls"));
            cliEnvironment.addCommand(new LocalPwd("lpwd"));
            cliEnvironment.addCommand(new LocalCd("lcd"));

            cliEnvironment.addCommand(new LocalHead("lhead"));
            cliEnvironment.addCommand(new LocalCat("lcat"));
            cliEnvironment.addCommand(new LocalMkdir("lmkdir"));
            cliEnvironment.addCommand(new LocalRm("lrm"));

            cliEnvironment.addCommand(new Use("use"));
            cliEnvironment.addCommand(new com.cloudera.utils.hadoop.hdfs.shell.command.List("list"));
            cliEnvironment.addCommand(new com.cloudera.utils.hadoop.hdfs.shell.command.List("nss"));
            cliEnvironment.addCommand(new List("namespaces"));
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
