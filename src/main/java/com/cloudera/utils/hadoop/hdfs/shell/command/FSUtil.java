/*
 * Copyright (c) 2022. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.hdfs.shell.command;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import org.apache.hadoop.fs.FileStatus;

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
        String retval = (file.isDirectory() ? ANSIStyle.style(file.getPath()
                        .getName(), ANSIStyle.FG_GREEN) : file.getPath()
                        .getName());

        return retval;
    }

//    public static void prompt(Environment env) {
//        try {
//            StringBuilder sb = new StringBuilder();
//            FileSystemState fss = env.getFileSystemOrganizer().getCurrentFileSystemState();;
//            FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
//
////            FileSystem localfs = (FileSystem) env.getValue(Constants.LOCAL_FS);
////            FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);
//
////            String hdfswd = hdfs.getWorkingDirectory().toString();
//            String hdfswd = fss.getWorkingDirectory().toString();
//            String localwd = lfss.getWorkingDirectory().toString();
//
//            String hwd = ANSIStyle.style(hdfswd, ANSIStyle.FG_GREEN) ;
//
//            String lwd = ANSIStyle.style(localwd, ANSIStyle.FG_YELLOW);
//
//            String lclPrompt = ANSIStyle.style("hdfs:>", ANSIStyle.FG_RED);
//
//            Configuration config = env.getConfig();
//            String defaultFS = config.get("fs.defaultFS");
//            String availableNamespaces = config.get("dfs.nameservices");
//            String[] namespaces = availableNamespaces.split(",");
//
//            StringBuilder prompt = new StringBuilder();
//            prompt.append(ANSIStyle.style("REMOTE: ", ANSIStyle.FG_BLUE) + hwd + "\t\t" + ANSIStyle.style("LOCAL: ", ANSIStyle.FG_BLUE) + lwd + "\n" + lclPrompt);
//
////            env.setCurrentPrompt(prompt.toString());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

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
