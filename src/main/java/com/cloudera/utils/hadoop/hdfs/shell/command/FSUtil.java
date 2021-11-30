/*
 *  Hadoop CLI
 *
 *  (c) 2016-2019 David W. Streever. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with David W. Streever, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with David W. Streever or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) David W. Streever PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) David W. Streever DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) David W. Streever IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 *  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, David W. Streever IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 *
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
