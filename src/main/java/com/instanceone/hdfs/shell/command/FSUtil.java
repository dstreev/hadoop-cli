// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;

import com.instanceone.hdfs.shell.format.ANSIStyle;

public class FSUtil {

    private FSUtil() {
    }

    public static String longFormat(FileStatus file) {
        String retval = (file.isDir() ? "d" : "-")
                        + file.getPermission()
                        + "  "
                        + file.getOwner()
                        + "  "
                        + file.getGroup()
                        + "  "
                        + formatFileSize(file.getLen())
                        + "  "
                        + formatDate(file.getModificationTime())

                        + "   "
                        + (file.isDir() ? ANSIStyle.style(file.getPath()
                                        .getName(), ANSIStyle.FG_BLUE) : file
                                        .getPath().getName());

        return retval;
    }

    public static String shortFormat(FileStatus file) {
        String retval = (file.isDir() ? ANSIStyle.style(file.getPath()
                        .getName(), ANSIStyle.FG_BLUE) : file.getPath()
                        .getName());

        return retval;
    }
    
    public static String formatDate(long millis){
        Date date = new Date(millis);
        return formatDate(date);
    }
    
    public static String formatDate(Date date){
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z");
        return df.format(date);
    }
    
    public static String formatFileSize(long size) {
        if(size <= 0) return "0";
        final String[] units = new String[] { "B", "K", "M", "G", "T" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups))  + units[digitGroups];
    }
}
