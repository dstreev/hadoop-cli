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

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.hdfs.shell.command.Constants;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import jline.console.completer.Completer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemNameCompleter implements Completer {
    private final Supplier<CliSession> sessionSupplier;
    private boolean local = false;

    public FileSystemNameCompleter(Supplier<CliSession> sessionSupplier) {
        this.sessionSupplier = sessionSupplier;
    }

    public FileSystemNameCompleter(Supplier<CliSession> sessionSupplier, boolean local) {
        this.sessionSupplier = sessionSupplier;
        this.local = local;
    }

    @SuppressWarnings("unused")
    private static String strip(String prefix, String target) {
        return target.substring(prefix.length());
    }

    protected void logv(CliSession session, String log) {
        if (session.isVerbose()) {
            System.out.println(log);
        }
    }

    protected void loge(String log) {
        System.err.println(log);
    }

    protected void logd(CliSession session, String log) {
        if (session.isDebug()) {
            System.out.println(log);
        }
    }

    // TODO add ability to handle ~/ for local filesystems
    // Remember...  the completers don't work in the IDE (IntelliJ)
    public int complete(String buffer, final int cursor,
                        final List<CharSequence> candidates) {
        CliSession session = sessionSupplier.get();
        if (session == null) return 0;

        // Remove directives from buffer.
        String checkBuffer = buffer;
        logd(session, ">>> Cursor: " + cursor + " Candidates: " + candidates.toString());

        if (checkBuffer == null) {
            logd(session, "Buffer null Cursor: " + cursor);
//            checkBuffer = "./";
        } else {
            logd(session, "Buffer: " + buffer + " Buffer Length: " + buffer.length() + " Cursor pos: " + cursor);

            if (checkBuffer.startsWith("-")) {
                // If the last item is a directive, remove it.
                checkBuffer = null;
            } else if (checkBuffer.contains(":")) {  // In cases of chmod IE: dstreev:dstreev
                checkBuffer = null;
            }

        }

        logd(session, "Check Buffer: " + checkBuffer);

        FileSystemState fss;
        FileSystem fs;

        String prefix;

        Path basePath = null;

        if (!local)
            fss = session.getFileSystemOrganizer().getCurrentFileSystemState();
        else
            fss = session.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);

        fs = fss.getFileSystem();
        prefix = fss.getURI();
        basePath = fss.getWorkingDirectory();

        if (fs == null) {
            return 0;
        }

        logd(session, "Prefix: " + prefix);

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

        logd(session, "Comp. Dir: " + completionDir);
        if (session.getFileSystemOrganizer().getDefaultOzoneFileSystemState() == fss) {
            // append the protocol
            completionDir = new Path(prefix, completionDir);
        }

        logd(session, "Comp. Dir: " + completionDir);

        // When we're not dealing with the default FS, we need exit.
        if (!session.getFileSystemOrganizer().isDefaultFileSystemState(fss)) {
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

            logd(session, "MatchedIndex: " + matchedIndex);

            // After we've handled candidate matches, we need to reset the index to
            // the original so items like 'dstreev:dstreev' as a param for
            // chmod aren't erased.
            if (buffer != null && (buffer.contains(":") || buffer.startsWith("-"))) {
                logd(session, "Overwriting MatchedIndex with cursor: " + cursor);
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
