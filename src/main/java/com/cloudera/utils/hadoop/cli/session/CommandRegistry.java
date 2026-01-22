/*
 * Copyright (c) 2022-2024. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.cli.session;

import com.cloudera.utils.hadoop.hdfs.shell.command.*;
import com.cloudera.utils.hadoop.hdfs.util.HdfsLsPlus;
import com.cloudera.utils.hadoop.hdfs.util.HdfsSource;
import com.cloudera.utils.hadoop.shell.command.Command;
import com.cloudera.utils.hadoop.shell.commands.Env;
import com.cloudera.utils.hadoop.shell.commands.Exit;
import com.cloudera.utils.hadoop.shell.commands.Help;
import com.cloudera.utils.hadoop.shell.commands.HistoryCmd;
import com.cloudera.utils.hadoop.yarn.ContainerStatsCommand;
import com.cloudera.utils.hadoop.yarn.SchedulerStatsCommand;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Component
/**
 * Registry for CLI commands. Extracted from CliEnvironment to allow
 * shared or per-session command sets.
 */
public class CommandRegistry {
    public CommandRegistry() {
        register(new HdfsCd("cd"));
        register(new HdfsPwd("pwd"));

        // remote local
        register(new HdfsCommand("get", Direction.REMOTE_LOCAL));
        register(new HdfsCommand("copyFromLocal", Direction.LOCAL_REMOTE));
        // local remote
        register(new HdfsCommand("put", Direction.LOCAL_REMOTE));
        register(new HdfsCommand("copyToLocal", Direction.REMOTE_LOCAL));
        // src dest
        register(new HdfsCommand("cp", Direction.REMOTE_REMOTE));

        // amend to context path, if present
        register(new HdfsCommand("chown", Direction.NONE, 1));
        register(new HdfsCommand("chmod", Direction.NONE, 1));
        register(new HdfsCommand("chgrp", Direction.NONE, 1));

        register(new HdfsAllowSnapshot("allowSnapshot", Direction.NONE, 1, false, true));
        register(new HdfsDisallowSnapshot("disallowSnapshot", Direction.NONE, 1, false, true));
        register(new HdfsLsSnapshottableDir("lsSnapshottableDir", Direction.NONE, 1, false, true));

        register(new HdfsCommand("createSnapshot"));
        register(new HdfsCommand("deleteSnapshot"));
        register(new HdfsCommand("renameSnapshot"));
        register(new SnapshotDiff("snapshotDiff"));

        register(new HdfsCommand("du", Direction.NONE));
        register(new HdfsCommand("df", Direction.NONE));
        register(new HdfsCommand("dus", Direction.NONE));
        register(new HdfsCommand("ls", Direction.NONE));
        register(new HdfsCommand("lsr", Direction.NONE));
//        env.addCommand(new HdfsCommand("find", env, Direction.NONE, 1, false));

        register(new HdfsCommand("mkdir", Direction.NONE));

        register(new HdfsCommand("count", Direction.NONE));
        register(new HdfsCommand("stat", Direction.NONE));
        register(new HdfsCommand("tail", Direction.NONE));
        register(new HdfsCommand("head", Direction.NONE));
        register(new HdfsCommand("touchz", Direction.NONE));

        register(new HdfsCommand("rm", Direction.NONE));
        register(new HdfsCommand("rmdir", Direction.NONE));
        register(new HdfsCommand("mv", Direction.REMOTE_REMOTE));
        register(new HdfsCommand("cat", Direction.NONE));
        register(new HdfsCommand("test", Direction.NONE));
        register(new HdfsCommand("text", Direction.NONE));
        register(new HdfsCommand("touchz", Direction.NONE));
        register(new HdfsCommand("checksum", Direction.NONE));

//        addCommand(new HdfsScan("scan", cliEnvironment));

//        addCommand(new HdfsCommand("usage", cliEnvironment));

        // Security Help
//        env.addCommand(new HdfsUGI("ugi"));
//        env.addCommand(new HdfsKrb("krb", env, Direction.NONE, 1));

        // HDFS Utils
        //env.addCommand(new HdfsRepair("repair", env, Direction.NONE, 2, true, true));

        register(new Env("env"));
        register(new HdfsConnect("connect"));
        register(new Help("help", this));
        register(new HistoryCmd("history"));

        // HDFS Tools
        register(new HdfsLsPlus("lsp", Direction.NONE));
//        addCommand(new HdfsNNStats("nnstat", cliEnvironment, Direction.NONE));

        register(new HdfsSource("source"));

        // MapReduce Tools
        // TODO: Add back once the field mappings are completed.
//        addCommand(new JhsStats("jhsstat", cliEnvironment, Direction.NONE));

        // Yarn Tools
        register(new ContainerStatsCommand("cstat", Direction.NONE));
        register(new SchedulerStatsCommand("sstat", Direction.NONE));

        register(new Exit("exit"));
        register(new LocalLs("lls"));
        register(new LocalPwd("lpwd"));
        register(new LocalCd("lcd"));

        register(new LocalHead("lhead"));
        register(new LocalCat("lcat"));
        register(new LocalMkdir("lmkdir"));
        register(new LocalRm("lrm"));

        register(new Use("use"));
        register(new com.cloudera.utils.hadoop.hdfs.shell.command.List("list"));
        register(new com.cloudera.utils.hadoop.hdfs.shell.command.List("nss"));
        register(new List("namespaces"));

    }
    private final Map<String, Command> commands = new TreeMap<>();

    public void register(Command cmd) {
        commands.put(cmd.getName(), cmd);
    }

    public Command getCommand(String name) {
        return commands.get(name);
    }

    public Set<String> commandList() {
        return commands.keySet();
    }

    public Map<String, Command> getCommands() {
        return commands;
    }
}
