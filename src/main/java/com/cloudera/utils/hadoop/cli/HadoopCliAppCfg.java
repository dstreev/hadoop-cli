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

import com.cloudera.utils.hadoop.cli.session.CommandRegistry;
import com.cloudera.utils.hadoop.hdfs.shell.command.*;
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

@Configuration
@ConditionalOnProperty(
        name = "hadoop-cli.api.enabled",
        havingValue = "false",
        matchIfMissing = true)
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

//    @Bean
//    @Order(10)
//    CommandLineRunner initHadoopConfiguration(CliEnvironment cliEnvironment) {
//        return args -> {
//            // TODO: Need to see if this attempts a connection to HDFS.
//            if (!cliEnvironment.isInitialized()) {
//                cliEnvironment.init();
//            }
//        };
//    }

//    @Bean
//    @Order(5)
    CommandLineRunner initCommands(CommandRegistry commandRegistry) {
        return args -> {
//            setBannerResource("/hadoop_banner_0.txt");

            commandRegistry.register(new HdfsCd("cd"));
            commandRegistry.register(new HdfsPwd("pwd"));

            // remote local
            commandRegistry.register(new HdfsCommand("get", Direction.REMOTE_LOCAL));
            commandRegistry.register(new HdfsCommand("copyFromLocal", Direction.LOCAL_REMOTE));
            // local remote
            commandRegistry.register(new HdfsCommand("put", Direction.LOCAL_REMOTE));
            commandRegistry.register(new HdfsCommand("copyToLocal", Direction.REMOTE_LOCAL));
            // src dest
            commandRegistry.register(new HdfsCommand("cp", Direction.REMOTE_REMOTE));

            // amend to context path, if present
            commandRegistry.register(new HdfsCommand("chown", Direction.NONE, 1));
            commandRegistry.register(new HdfsCommand("chmod", Direction.NONE, 1));
            commandRegistry.register(new HdfsCommand("chgrp", Direction.NONE, 1));

            commandRegistry.register(new HdfsAllowSnapshot("allowSnapshot", Direction.NONE, 1, false, true));
            commandRegistry.register(new HdfsDisallowSnapshot("disallowSnapshot", Direction.NONE, 1, false, true));
            commandRegistry.register(new HdfsLsSnapshottableDir("lsSnapshottableDir", Direction.NONE, 1, false, true));

            commandRegistry.register(new HdfsCommand("createSnapshot"));
            commandRegistry.register(new HdfsCommand("deleteSnapshot"));
            commandRegistry.register(new HdfsCommand("renameSnapshot"));
            commandRegistry.register(new SnapshotDiff("snapshotDiff"));

            commandRegistry.register(new HdfsCommand("du", Direction.NONE));
            commandRegistry.register(new HdfsCommand("df", Direction.NONE));
            commandRegistry.register(new HdfsCommand("dus", Direction.NONE));
            commandRegistry.register(new HdfsCommand("ls", Direction.NONE));
            commandRegistry.register(new HdfsCommand("lsr", Direction.NONE));
//        env.addCommand(new HdfsCommand("find", env, Direction.NONE, 1, false));

            commandRegistry.register(new HdfsCommand("mkdir", Direction.NONE));

            commandRegistry.register(new HdfsCommand("count", Direction.NONE));
            commandRegistry.register(new HdfsCommand("stat", Direction.NONE));
            commandRegistry.register(new HdfsCommand("tail", Direction.NONE));
            commandRegistry.register(new HdfsCommand("head", Direction.NONE));
            commandRegistry.register(new HdfsCommand("touchz", Direction.NONE));

            commandRegistry.register(new HdfsCommand("rm", Direction.NONE));
            commandRegistry.register(new HdfsCommand("rmdir", Direction.NONE));
            commandRegistry.register(new HdfsCommand("mv", Direction.REMOTE_REMOTE));
            commandRegistry.register(new HdfsCommand("cat", Direction.NONE));
            commandRegistry.register(new HdfsCommand("test", Direction.NONE));
            commandRegistry.register(new HdfsCommand("text", Direction.NONE));
            commandRegistry.register(new HdfsCommand("touchz", Direction.NONE));
            commandRegistry.register(new HdfsCommand("checksum", Direction.NONE));

//        addCommand(new HdfsScan("scan", cliEnvironment));

//        addCommand(new HdfsCommand("usage", cliEnvironment));

            // Security Help
//        env.addCommand(new HdfsUGI("ugi"));
//        env.addCommand(new HdfsKrb("krb", env, Direction.NONE, 1));

            // HDFS Utils
            //env.addCommand(new HdfsRepair("repair", env, Direction.NONE, 2, true, true));

            commandRegistry.register(new Env("env"));
            commandRegistry.register(new HdfsConnect("connect"));
            commandRegistry.register(new Help("help", commandRegistry));
            commandRegistry.register(new HistoryCmd("history"));

            // HDFS Tools
            commandRegistry.register(new HdfsLsPlus("lsp", Direction.NONE));
//        addCommand(new HdfsNNStats("nnstat", cliEnvironment, Direction.NONE));

            commandRegistry.register(new HdfsSource("source"));

            // MapReduce Tools
            // TODO: Add back once the field mappings are completed.
//        addCommand(new JhsStats("jhsstat", cliEnvironment, Direction.NONE));

            // Yarn Tools
            commandRegistry.register(new ContainerStatsCommand("cstat", Direction.NONE));
            commandRegistry.register(new SchedulerStatsCommand("sstat", Direction.NONE));

            commandRegistry.register(new Exit("exit"));
            commandRegistry.register(new LocalLs("lls"));
            commandRegistry.register(new LocalPwd("lpwd"));
            commandRegistry.register(new LocalCd("lcd"));

            commandRegistry.register(new LocalHead("lhead"));
            commandRegistry.register(new LocalCat("lcat"));
            commandRegistry.register(new LocalMkdir("lmkdir"));
            commandRegistry.register(new LocalRm("lrm"));

            commandRegistry.register(new Use("use"));
            commandRegistry.register(new com.cloudera.utils.hadoop.hdfs.shell.command.List("list"));
            commandRegistry.register(new com.cloudera.utils.hadoop.hdfs.shell.command.List("nss"));
            commandRegistry.register(new List("namespaces"));
        };
    }

    @Bean
    @Order(100)
    CommandLineRunner runInteractiveShell(Shell shell) {
        return args -> {
//            if (!cliEnvironment.isApiMode()) {
                log.info("Launching Interactive Shell");
                try {
                    shell.startShell();
                } catch (DisabledException e) {
                    throw new RuntimeException(e);
                }
//            }
        };
    }

}
