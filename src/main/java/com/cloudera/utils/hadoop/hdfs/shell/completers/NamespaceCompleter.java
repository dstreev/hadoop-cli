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

package com.cloudera.utils.hadoop.hdfs.shell.completers;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import jline.console.completer.Completer;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;

public class NamespaceCompleter implements Completer {
    private final CliEnvironment env;

    public NamespaceCompleter(CliEnvironment env) {
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
