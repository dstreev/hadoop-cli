package com.instanceone.hdfs.shell.completers;

import java.io.File;
import java.io.IOException;
import java.util.List;

import jline.console.completer.Completer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;
import com.instanceone.hdfs.shell.command.HdfsCommand;


public class FileSystemNameCompleter implements Completer {
    private Environment env;



    public FileSystemNameCompleter(Environment env) {
        // this.includeFiles = includeFiles;
        this.env = env;
    }

    private static String strip(String prefix, String target) {
        return target.substring(prefix.length());
    }

    public int complete(String buffer, final int cursor,
                    final List<CharSequence> candidates) {

        FileSystem fs = (FileSystem) env.getValue(HdfsCommand.HDFS);
        String prefix = env.getProperty(HdfsCommand.HDFS_URL);
//        System.out.println(prefix);

        Path basePath = fs.getWorkingDirectory();
        String curPath = strip(prefix, basePath.toString());
//        System.out.println("curPath: " + curPath);

        if (buffer == null) {
//            System.out.println("Buffer was null!");
            buffer = "./";
        }


//        System.out.println("Match: '" + buffer + "'");
//        System.out.println("Base Path: " + basePath);

        Path completionPath = buffer.startsWith("/") ? new Path(prefix, buffer)
                        : new Path(basePath, buffer);
//        System.out.println("Comp. Path: " + completionPath);
//        System.out.println("Comp. Parent: " + completionPath.getParent());
        Path completionDir = (completionPath.getParent() == null || buffer.endsWith("/")) ? completionPath
                        : completionPath.getParent();
//        System.out.println("Comp. Dir: " + completionDir);
        try {
            FileStatus[] entries = fs.listStatus(completionDir);
//            System.out.println("Possible matches:");
//            for (FileStatus fStat : entries) {
//                System.out.println(fStat.getPath().getName());
//                if(fStat.getPath().toString().startsWith(completionPath.toString())){
//                    System.out.println("^ WOOP!");
//                }
//            }
            
            return matchFiles(buffer, completionPath.toString(), entries, candidates);

        }
        catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    protected String separator() {
        return "/";
    }

    protected int matchFiles(final String buffer, final String translated,
                    final FileStatus[] files,
                    final List<CharSequence> candidates) {
        if (files == null) {
            return -1;
        }

        int matches = 0;

        // first pass: just count the matches
        for (FileStatus file : files) {
            // System.out.println("Checking: " + file.getPath());
            if (file.getPath().toString().startsWith(translated)) {
                // System.out.println("Found match: " + file.getPath());
                matches++;
            }
        }
        for (FileStatus file : files) {
            if (file.getPath().toString().startsWith(translated)) {
                String name = file.getPath().getName()
                                + (matches == 1 && file.isDir() ? separator()
                                                : " ");
                // System.out.println("Adding candidate: " + name);
                candidates.add(name);
            }
        }

        final int index = buffer.lastIndexOf(separator());

        return index + separator().length();
    }

}
