// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.regex.Pattern;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class HdfsPut extends HdfsCommand {

    public HdfsPut(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {

        try {
//            String cwd = super.cwd(env, reader);
//            String lcwd = env.getProperty(Environment.CWD);
//            String hdfsCwd = env.getProperty(HDFS_CWD);
//            FileSystem fs = super.getFileSystem(env, reader);
//            
//            FileSystem local = FileSystem.getLocal(new Configuration());
//            local.setWorkingDirectory(new Path("file:" + lcwd));
//            
//            log(cmd, "Local Dir: " + lcwd);
//            log(cmd,"HDFS Dir: " + hdfsCwd);
            
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
    
    /*
    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {

        try {
            String cwd = super.cwd(env, reader);
            String lcwd = env.getProperty(Environment.CWD);
            String hdfsCwd = env.getProperty(HDFS_CWD);
            FileSystem fs = super.getFileSystem(env, reader);
            
            FileSystem local = FileSystem.getLocal(new Configuration());
            local.setWorkingDirectory(new Path("file:" + lcwd));
            
            log(cmd, "Local Dir: " + lcwd);
            log(cmd,"HDFS Dir: " + hdfsCwd);
            
            String localFile = cmd.getArgs()[0];
            
            
            String localFileRegex = localFile.replaceAll("\\*", ".*");
            FilenameFilter regexFilter = new RegexFilenameFilter(localFileRegex);
            File cwdFile = new File(lcwd);
            File[] files = cwdFile.listFiles(regexFilter);
            Path[] filesToUpload = new Path[files.length];
            for(int i = 0; i<files.length;i++){
                File file = files[i];
                log(cmd,"Matching file: " + file);
                filesToUpload[i] = new Path(file.getName());
                
            }

            Path hdfsPath = cmd.getArgs().length > 1 ? new Path(hdfsCwd, cmd.getArgs()[1]) : new Path(hdfsCwd);
            log(cmd,"Remote path: " + hdfsPath);
            
            fs.copyFromLocalFile(false, false, filesToUpload, hdfsPath);
            
            
            
            
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }
     */
    
    public static void main(String[] args) throws Exception{
        String file = "*.xml";
        String localFileRegex = file.replaceAll("\\*", ".*");
        
       
        System.out.println(localFileRegex);
        
        System.out.println( Pattern.matches(localFileRegex, "pom.xml"));
        
        
    }
    
    
    
    
    @Override
    public Completer getCompleter() {
        return new FileNameCompleter();
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
