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

import java.io.IOException;
import java.util.List;

import com.cloudera.utils.hadoop.hdfs.shell.command.Constants;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import jline.console.completer.Completer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.utils.hadoop.shell.Environment;

public class FileSystemNameCompleter implements Completer {
    private Environment env;
    private boolean local = false;

    public FileSystemNameCompleter(Environment env) {
        // this.includeFiles = includeFiles;
        this.env = env;
    }

    public FileSystemNameCompleter(Environment env, boolean local) {
        this.env = env;
        this.local = local;
    }

    @SuppressWarnings("unused")
    private static String strip(String prefix, String target) {
        return target.substring(prefix.length());
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
        String checkBuffer = buffer;
        logd(">>> Cursor: " + cursor + " Candidates: " + candidates.toString());

        if (checkBuffer == null) {
            logd("Buffer null Cursor: " + cursor);
//            checkBuffer = "./";
        } else {
            logd("Buffer: " + buffer + " Buffer Length: " + buffer.length() + " Cursor pos: " + cursor);

            if (checkBuffer.startsWith("-")) {
                // If the last item is a directive, remove it.
                checkBuffer = null;
            } else if (checkBuffer.contains(":")) {  // In cases of chmod IE: dstreev:dstreev
                checkBuffer = null;
            }

        }

        logd("Check Buffer: " + checkBuffer);

        FileSystemState fss;
        FileSystem fs;

        String prefix;

        Path basePath = null;

        if (!local)
            fss = env.getFileSystemOrganizer().getCurrentFileSystemState();
        else
            fss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);

        fs = fss.getFileSystem();
        prefix = fss.getURI();
        basePath = fss.getWorkingDirectory();

        if (fs == null) {
            return 0;
        }

        logd("Prefix: " + prefix);

        Path completionDir = null;

        if (checkBuffer != null) {
            if (checkBuffer.startsWith("/")) {
                // Absolute location
                basePath = new Path("/");
                if (checkBuffer.length() == 1) {
                    completionDir = basePath;
                } else {
                    if (checkBuffer.endsWith("/")) {
                        completionDir = new Path(basePath, checkBuffer.substring(1));
                    } else if (checkBuffer.substring(1).contains("/")) {
                        completionDir = new Path(basePath, checkBuffer).getParent();
                    } else {
                        completionDir = basePath;
                    }
                }
            } else if (checkBuffer.contains("/")) {
                if (checkBuffer.endsWith("/")) {
                    completionDir = new Path(basePath, checkBuffer);
                } else {
                    completionDir = new Path(basePath, checkBuffer).getParent();
                }
            } else {
                completionDir = basePath;
            }

        } else {
            completionDir = fss.getWorkingDirectory();
            checkBuffer = "";
        }

        logd("Comp. Dir: " + completionDir);
        if (env.getFileSystemOrganizer().getDefaultOzoneFileSystemState() == fss) {
            // append the protocol
            completionDir = new Path(prefix, completionDir);
        }

        logd("Comp. Dir: " + completionDir);

        // When we're not dealing with the default FS, we need exit.
        if (!env.getFileSystemOrganizer().isDefaultFileSystemState(fss)) {
            return -1;
        }

        try {
            // TODO: Need to replace with getContentSummary.  fs.listStatus doesn't appear to be
            //       a supported hcfs interface.  Because it fails when going against ofs.
            FileStatus[] entries = fs.listStatus(completionDir);

            int matchedIndex = matchFiles(prefix, checkBuffer, completionDir.toString(), entries,
                    candidates);

            // now that we have a matched index, we need to pull it back to the directory level
            // and let jline fill in with matching candidates.
            if (basePath.toString().equals(separator())) {
                // When Absolute
                if (!checkBuffer.substring(1).contains(separator())) {
                    // absolute with no sub dir(s)
                    matchedIndex = 1;
                } else {
                    // absolute with subdirs
                    matchedIndex += completionDir.toString().length() - checkBuffer.length();
                }
            } else {
                // Relative
                if (!checkBuffer.contains(separator())) {
                    matchedIndex = completionDir.toString().length() - basePath.toString().length();
                } else {
                    matchedIndex += checkBuffer.lastIndexOf(separator()) - checkBuffer.length();
                }
            }

            logd("MatchedIndex: " + matchedIndex);

            // After we've handled candidate matches, we need to reset the index to
            // the original so items like 'dstreev:dstreev' as a param for
            // chmod aren't erased.
            if (buffer != null && (buffer.contains(":") || buffer.startsWith("-"))) {
                logd("Overwriting MatchedIndex with cursor: " + cursor);
                matchedIndex = cursor;
            }

            return matchedIndex;

        } catch (IOException e) {
            loge(e.getMessage());
            //e.printStackTrace();
            return -1;
        }
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

        String bufferCheck = null;
//                buffer.length() > 0 ? translated + separator() + buffer : translated

        if (buffer != null && buffer.length() > 0) {
            if (buffer.startsWith(separator())) {// && translated.endsWith("/")) {
                bufferCheck = buffer;
            } else if (buffer.contains(separator())) {
                if (buffer.endsWith(separator())) {
                    bufferCheck = translated;
                } else {
                    bufferCheck = translated + buffer.substring(buffer.indexOf(separator()));
                }
            } else {
                bufferCheck = translated + separator() + buffer;
            }
        } else {
            bufferCheck = translated + separator();
        }

        // first pass: just count the matches
        for (FileStatus file : files) {
            // System.out.println("Checking: " + file.getPath());
            String checkLocation = file.getPath().toString().substring(prefix.length());
            if (checkLocation.startsWith(bufferCheck)) {
                // System.out.println("Found match: " + file.getPath());
                matches++;
            }
        }

        for (FileStatus file : files) {
            String checkLocation = file.getPath().toString().substring(prefix.length());
            if (checkLocation.startsWith(bufferCheck)) {
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
