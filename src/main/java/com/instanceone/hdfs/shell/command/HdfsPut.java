// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;
import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;

public class HdfsPut extends HdfsCommand {
    private Environment env;

    public HdfsPut(String name, Environment env) {
        super(name);
        this.env = env;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {

        try {
            
            FileSystem hdfs = (FileSystem)env.getValue(HDFS);
            FileSystem localfs = (FileSystem)env.getValue(LOCAL_FS);
            String localFile = cmd.getArgs()[0];
            
            
            String localFileRegex = localFile.replaceAll("\\*", ".*");
            FilenameFilter regexFilter = new RegexFilenameFilter(localFileRegex);
            File cwdFile = new File(localfs.getWorkingDirectory().toString().substring(5));
            File[] files = cwdFile.listFiles(regexFilter);
            Path[] filesToUpload = new Path[files.length];
            for(int i = 0; i<files.length;i++){
                File file = files[i];
                log(cmd,"Matching file: " + file);
                filesToUpload[i] = new Path(file.getName());
                
            }

            Path hdfsPath = cmd.getArgs().length > 1 ? new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[1]) : hdfs.getWorkingDirectory();
            log(cmd,"Remote path: " + hdfsPath);
            
            hdfs.copyFromLocalFile(false, false, filesToUpload, hdfsPath);

        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }
    

    
    public static void main(String[] args) throws Exception{
        String file = "*.xml";
        String localFileRegex = file.replaceAll("\\*", ".*");
        
       
        System.out.println(localFileRegex);
        
        System.out.println( Pattern.matches(localFileRegex, "pom.xml"));
        
        
    }
    
    
    
    
    @Override
    public Completer getCompleter() {
        ArrayList<Completer> completers = new ArrayList<Completer>();
        completers.add(new FileSystemNameCompleter(env, true));
        completers.add(new FileSystemNameCompleter(env, false));
        ArgumentCompleter completer = new ArgumentCompleter(completers);
        
        return completer;
    }




    public static class RegexFilenameFilter implements FilenameFilter {
        
        private String regex;
        
        public RegexFilenameFilter(String regex){
            this.regex = regex;
        }

        public boolean accept(File dir, String name) {
            return Pattern.matches(this.regex, name);
        }
        
    }

}
