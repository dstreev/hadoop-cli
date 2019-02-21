// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.streever.hadoop.hdfs.shell.command;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.streever.tools.stemshell.Environment;
import org.apache.hadoop.fs.FileStatus;

import com.streever.tools.stemshell.format.ANSIStyle;
import org.apache.hadoop.fs.FileSystem;

public class FSUtil {

    private FSUtil() {
    }

    public static String longFormat(FileStatus file) {
        String retval = (file.isDir() ? "d" : "-")
                        + file.getPermission()
                        + (file.getPermission().getAclBit() ? "+":"")
                        + (file.getPermission().getEncryptedBit() ? "#":"")
                        + "\t"
                        + file.getOwner()
                        + "\t"
                        + file.getGroup()
                        + "\t"
                        + formatFileSize(file.getLen())
                        + "\t"
                        + formatDate(file.getModificationTime())

                        + "\t"
                        + (file.isDir() ? ANSIStyle.style(file.getPath()
                                        .getName(), ANSIStyle.FG_GREEN) : file
                                        .getPath().getName());

        return retval;
    }

    public static String shortFormat(FileStatus file) {
        String retval = (file.isDir() ? ANSIStyle.style(file.getPath()
                        .getName(), ANSIStyle.FG_GREEN) : file.getPath()
                        .getName());

        return retval;
    }

    public static void prompt(Environment env) {
        try {
            StringBuilder sb = new StringBuilder();

            FileSystem localfs = (FileSystem) env.getValue(Constants.LOCAL_FS);
            FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);

            String hdfswd = hdfs.getWorkingDirectory().toString();
            String localwd = localfs.getWorkingDirectory().toString();

            String hwd = ANSIStyle.style(hdfswd, ANSIStyle.FG_GREEN) ;

            String lwd = ANSIStyle.style(localwd, ANSIStyle.FG_YELLOW);

            env.setPrompt(" " + hwd + "\n " + lwd + "\n$ ");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String formatDate(long millis){
        Date date = new Date(millis);
        return formatDate(date);
    }
    
    public static String formatDate(Date date){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        return df.format(date);
    }
    
    public static String formatFileSize(long size) {
        if(size <= 0) return "0";
        final String[] units = new String[] { "B", "K", "M", "G", "T" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups))  + units[digitGroups];
    }
}
