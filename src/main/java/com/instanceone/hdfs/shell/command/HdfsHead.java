// Copyright (c) 2012 Health Market Science, Inc.

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

import com.instanceone.hdfs.shell.Environment;
import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;

public class HdfsHead extends HdfsCommand {
    
    public static final int LINE_COUNT = 10;
    
    private Environment env;

    public HdfsHead(String name, Environment env) {
        super(name);
        this.env = env;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader console) {
        FileSystem hdfs = (FileSystem)env.getValue(HDFS);
        
        if(cmd.getArgs().length == 1){
            int lineCount = Integer.parseInt(cmd.getOptionValue("n", String.valueOf(LINE_COUNT)));
            Path path = new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[0]);
            BufferedReader reader = null;
            try {
                InputStream is = hdfs.open(path);
                InputStreamReader isr = new InputStreamReader(is);
                reader = new BufferedReader(isr);
                String line = null;
                for(int i = 0; ((i <= lineCount) && (line = reader.readLine()) != null);i++ ){
                    System.out.println(line);
                }
            }
            catch (IOException e) {
                System.out.println("Error reading file '" + cmd.getArgs()[0] + "': " + e.getMessage());
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
        // TODO Auto-generated method stub
        Options opts = super.getOptions();
        opts.addOption("n", true, "number of lines to display (defaults to 10)");
        return opts;
    }
    
    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }

}
