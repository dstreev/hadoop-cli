// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
    
    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        //config.set("fs.default.name", "hdfs://dlcirrus01:9000/");
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000/"),config);
//        hdfs.
        FileSystem localfs = FileSystem.get(URI.create("file:/"), config);
        Path srcPath = new Path("/");
        FileStatus[] files = hdfs.listStatus(srcPath);
        for(FileStatus file : files){
            System.out.println();
            //System.out.println(file.toString());
            System.out.println(file.getPermission() + " " + file.getOwner() + ":" + file.getGroup() + " " + file.getPath());
        }
        
        files = localfs.listStatus(srcPath);
        for(FileStatus file : files){
            System.out.println();
            //System.out.println(file.toString());
            System.out.println(file.getPermission() + " " + file.getOwner() + ":" + file.getGroup() + " " + file.getPath());
        }
    }

}
