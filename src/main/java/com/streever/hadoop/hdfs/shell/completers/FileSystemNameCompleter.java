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

package com.streever.hadoop.hdfs.shell.completers;

import java.io.IOException;
import java.util.List;

import com.streever.hadoop.hdfs.shell.command.Constants;
import jline.console.completer.Completer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.hadoop.shell.Environment;

public class FileSystemNameCompleter implements Completer {
    private Environment env;
    private boolean local = false;

    public FileSystemNameCompleter(Environment env, boolean local) {
        // this.includeFiles = includeFiles;
        this.env = env;
        this.local = local;
    }

    @SuppressWarnings("unused")
    private static String strip(String prefix, String target) {
        return target.substring(prefix.length());
    }

    protected void logv(String log){
        if(env.isVerbose()){
            System.out.println(log);
        }
    }

    protected void logd(String log){
        if(env.isDebug()){
            System.out.println(log);
        }
    }

    // TODO add ability to handle ~/ for local filesystems
    // Remember...  the completers don't work in the IDE (IntelliJ)
    public int complete(String buffer, final int cursor,
                    final List<CharSequence> candidates) {

        // Remove directives from buffer.
        String checkBuffer = buffer;
        logd(">>> Cursor: "+ cursor + " Candidates: " + candidates.toString());
        
        if (checkBuffer == null) {
            logd("Buffer null Cursor: " + cursor);
            checkBuffer = "./";
        } else {
            logd("Buffer: " + buffer + " Buffer Length: " + buffer.length() + " Cursor pos: " + cursor);

            if (checkBuffer.startsWith("-")) {
                // If the last item is a directive, remove it.
                checkBuffer = "./";
            } else if (checkBuffer.contains(":")) {  // In cases of chmod IE: dstreev:dstreev
                checkBuffer = "./";
            }

        }

        logd("Check Buffer: " + checkBuffer);
        
        FileSystem fs;

        String prefix;

        Path basePath = null;
        if (!this.local) {
            fs = (FileSystem) env.getValue(Constants.HDFS);
            prefix = env.getProperties().getProperty(Constants.HDFS_URL);
            basePath = env.getRemoteWorkingDirectory();
        }
        else {
            fs = (FileSystem) env.getValue(Constants.LOCAL_FS);
            prefix = "file:" + (checkBuffer != null && checkBuffer.startsWith("/") ? "/" : "");
            basePath = fs.getWorkingDirectory();
        }
        if(fs == null){
//            System.out.println("Not connected.");
            return 0;
        }
        logd("Prefix: " + prefix);

//        Path basePath = fs.getWorkingDirectory();
//        Path basePath = env.getWorkingDirectory();

        logd("Current Path: " + strip(prefix, basePath.toString()));

        if (checkBuffer == null) {
            // System.out.println("Buffer was null!");
            checkBuffer = "./";
        }

        Path completionPath = checkBuffer.startsWith("/") ? new Path(prefix, checkBuffer)
                        : new Path(basePath, checkBuffer);

        logd("Comp. Path: " + completionPath);
        logd("Comp. Parent: " + completionPath.getParent());

        Path completionDir = (completionPath.getParent() == null || checkBuffer
                        .endsWith("/")) ? completionPath : completionPath
                        .getParent();
        logd("Comp. Dir: " + completionDir);
        try {
            FileStatus[] entries = fs.listStatus(completionDir);
            // System.out.println("Possible matches:");
            // for (FileStatus fStat : entries) {
            // System.out.println(fStat.getPath().getName());
            // if(fStat.getPath().toString().startsWith(completionPath.toString())){
            // System.out.println("^ WOOP!");
            // }
            // }
            int matchedIndex = matchFiles(checkBuffer, completionPath.toString(), entries,
                            candidates);
            logd("MatchedIndex: " + matchedIndex);

            // After we've handled candidate matches, we need to reset the index to
            // the original so items like 'dstreev:dstreev' as a param for
            // chmod aren't erased.
            if (buffer != null && (buffer.contains(":") || buffer.startsWith("-"))) {
                logd("Overwriting MatchedIndex with cursor: " + cursor);
                matchedIndex = cursor;
            }

            return matchedIndex;

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
