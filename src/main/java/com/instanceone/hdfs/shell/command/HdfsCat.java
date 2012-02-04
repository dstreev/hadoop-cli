// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;
import com.instanceone.stemshell.Environment;

public class HdfsCat extends HdfsCommand {
    
    public static final int LINE_COUNT = 10;
    
    private Environment env;
    private boolean local = false;

    public HdfsCat(String name, Environment env, boolean local) {
        super(name);
        this.env = env;
        this.local = local;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader console) {
        FileSystem hdfs = this.local ? (FileSystem)env.getValue(LOCAL_FS) : (FileSystem)env.getValue(HDFS);
        logv(cmd, "CWD: " + hdfs.getWorkingDirectory());
        
        if(cmd.getArgs().length == 1){
            Path path = new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[0]);
            BufferedReader reader = null;
            try {
                InputStream is = hdfs.open(path);
                InputStreamReader isr = new InputStreamReader(is);
                reader = new BufferedReader(isr);
                String line = null;
                for(int i = 0; (line = reader.readLine()) != null;i++ ){
                    log(cmd, line);
                }
            }
            catch (IOException e) {
                log(cmd, "Error reading file '" + cmd.getArgs()[0] + "': " + e.getMessage());
            } finally{
                try {
                    if(reader != null){
                        reader.close();
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else{
//            usage();
        }
        

    }
    
    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        return opts;
    }
    
    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, this.local);
    }

}
