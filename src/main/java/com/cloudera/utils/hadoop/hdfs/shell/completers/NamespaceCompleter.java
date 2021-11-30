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

package com.cloudera.utils.hadoop.hdfs.shell.completers;

import com.cloudera.utils.hadoop.shell.Environment;
import jline.console.completer.Completer;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;

public class NamespaceCompleter implements Completer {
    private Environment env;

    public NamespaceCompleter(Environment env) {
        this.env = env;
    }

    protected void logv(String log) {
        if (env.isVerbose()) {
            System.out.println(log);
        }
    }

    protected void loge(String log) {
        System.err.println(log);
    }

    protected void logd(String log) {
        if (env.isDebug()) {
            System.out.println(log);
        }
    }

    // TODO add ability to handle ~/ for local filesystems
    // Remember...  the completers don't work in the IDE (IntelliJ)
    public int complete(String buffer, final int cursor,
                        final List<CharSequence> candidates) {

        // Remove directives from buffer.
//        String checkBuffer = buffer;
//        logd(">>> Cursor: " + cursor + " Candidates: " + candidates.toString());
//
//        if (checkBuffer == null) {
//            logd("Buffer null Cursor: " + cursor);
////            checkBuffer = "./";
//        } else {
//            logd("Buffer: " + buffer + " Buffer Length: " + buffer.length() + " Cursor pos: " + cursor);
//
//            if (checkBuffer.startsWith("-")) {
//                // If the last item is a directive, remove it.
//                checkBuffer = null;
//            } else if (checkBuffer.contains(":")) {  // In cases of chmod IE: dstreev:dstreev
//                checkBuffer = null;
//            }
//
//        }
//
//        logd("Check Buffer: " + checkBuffer);
//
//        FileSystemState fss;
//        FileSystem fs;
//
//        String prefix;
//
//        Path basePath = null;
//
//        if (!local)
//            fss = env.getFileSystemOrganizer().getCurrentFileSystemState();
//        else
//            fss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
//
//        fs = fss.getFileSystem();
//        prefix = fss.getURI();
//        basePath = fs.getWorkingDirectory();
//
//        if (fs == null) {
//            return 0;
//        }
//
//        logd("Prefix: " + prefix);
//
//        Path completionDir = null;
//
//        if (checkBuffer != null) {
//            if (checkBuffer.endsWith("/")) {
//                // Means its a directory.
//                completionDir = new Path(basePath, checkBuffer);
//            } else if (checkBuffer.contains("/")) {
//                // Means the buffer is a sub directory.
//                completionDir = new Path(basePath, checkBuffer).getParent();
//
//                int lastIndex = checkBuffer.lastIndexOf("/");
//                checkBuffer = checkBuffer.substring(lastIndex);
//
//            } else {
//                completionDir = basePath;
//            }
//
//        } else {
//            completionDir = fs.getWorkingDirectory();
//            checkBuffer = "";
//        }
//
//        logd("Comp. Dir: " + completionDir);
//        try {
//            FileStatus[] entries = fs.listStatus(completionDir);
//
//            int matchedIndex = matchFiles(prefix, checkBuffer, completionDir.toString(), entries,
//                    candidates);
//            matchedIndex += completionDir.toString().length() - basePath.toString().length();
//
//            logd("MatchedIndex: " + matchedIndex);
//
//            // After we've handled candidate matches, we need to reset the index to
//            // the original so items like 'dstreev:dstreev' as a param for
//            // chmod aren't erased.
//            if (buffer != null && (buffer.contains(":") || buffer.startsWith("-"))) {
//                logd("Overwriting MatchedIndex with cursor: " + cursor);
//                matchedIndex = cursor;
//            }
//
//            return matchedIndex;
//
//        } catch (IOException e) {
//            loge(e.getMessage());
//            //e.printStackTrace();
//            return -1;
//        }
        return -1;
    }

    protected String separator() {
        return "/";
    }

    protected int matchFiles(final String prefix, final String buffer, final String translated,
                             final FileStatus[] files,
                             final List<CharSequence> candidates) {
        if (files == null) {
            return -1;
        }

        int matches = 0;

        // first pass: just count the matches
        for (FileStatus file : files) {
            // System.out.println("Checking: " + file.getPath());
            String checkLocation = file.getPath().toString().substring(prefix.length());
            if (checkLocation.startsWith(translated + separator() + buffer)) {
                // System.out.println("Found match: " + file.getPath());
                matches++;
            }
        }
        for (FileStatus file : files) {
            String checkLocation = file.getPath().toString().substring(prefix.length());
            if (checkLocation.startsWith(translated + separator() + buffer)) {
                String name = file.getPath().getName()
                        + (matches == 1 && file.isDirectory() ? separator()
                        : " ");
                // System.out.println("Adding candidate: " + name);
                candidates.add(name);
            }
        }
        int index = 0;

        if (buffer != null)
            // Advance to last directory in buffer.
            index = buffer.length();

//        if (index == 0) {
//        }
//        final int index = buffer.lastIndexOf(separator());

        return index + separator().length();
    }

}
