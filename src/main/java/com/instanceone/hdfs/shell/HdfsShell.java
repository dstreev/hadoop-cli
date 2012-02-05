// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell;

import com.instanceone.hdfs.shell.command.HdfsCat;
import com.instanceone.hdfs.shell.command.HdfsCd;
import com.instanceone.hdfs.shell.command.HdfsConnect;
import com.instanceone.hdfs.shell.command.HdfsHead;
import com.instanceone.hdfs.shell.command.HdfsLs;
import com.instanceone.hdfs.shell.command.HdfsMkdir;
import com.instanceone.hdfs.shell.command.HdfsPut;
import com.instanceone.hdfs.shell.command.HdfsPwd;
import com.instanceone.hdfs.shell.command.HdfsRm;
import com.instanceone.hdfs.shell.command.LocalCd;
import com.instanceone.hdfs.shell.command.LocalLs;
import com.instanceone.hdfs.shell.command.LocalPwd;
import com.instanceone.stemshell.Environment;
import com.instanceone.stemshell.commands.Env;
import com.instanceone.stemshell.commands.Exit;
import com.instanceone.stemshell.commands.Help;
import com.instanceone.stemshell.commands.HistoryCmd;

public class HdfsShell extends com.instanceone.stemshell.Shell{
    
    public static void main(String[] args) throws Exception{
        new HdfsShell().run(args);
    }

    @Override
    public void initialize(Environment env) throws Exception {
        
        env.addCommand(new Exit("exit"));
        env.addCommand(new LocalLs("lls", env));
        env.addCommand(new LocalPwd("lpwd"));
        env.addCommand(new LocalCd("lcd", env));
        env.addCommand(new HdfsLs("ls"));
        env.addCommand(new HdfsCd("cd", env));
        env.addCommand(new HdfsPwd("pwd"));
        env.addCommand(new HdfsPut("put", env));
        env.addCommand(new HdfsHead("head", env, false));
        env.addCommand(new HdfsHead("lhead", env, true));
        env.addCommand(new HdfsCat("cat", env, false));
        env.addCommand(new HdfsCat("lcat", env, true));
        env.addCommand(new HdfsMkdir("mkdir", env, false));
        env.addCommand(new HdfsMkdir("lmkdir", env, true));
        env.addCommand(new HdfsRm("rm", false));
        env.addCommand(new HdfsRm("lrm", true));
        env.addCommand(new Env("env"));
        env.addCommand(new HdfsConnect("connect"));
        env.addCommand(new Help("help", env));
        env.addCommand(new HistoryCmd("history"));
        
    }

    @Override
    public String getName() {
        return "hdfs-cli";
    }

}
