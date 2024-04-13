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

package com.cloudera.utils.hadoop.shell.command;

import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.*;

@Slf4j
public class CommandReturn {
    public static final int GOOD = 0;
    public static final int BAD = -1;

    private int code = 0;
    private String[] commandArgs = null;
    private String path = null;
    private String error = null;
    private final List<List<Object>> records = new ArrayList<List<Object>>();

    private final ByteArrayOutputStream baosOut = new ByteArrayOutputStream();
    private final ByteArrayOutputStream baosErr = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(baosOut);
    private final PrintStream err = new PrintStream(baosErr);

    /*
    Style Map. Ordered Map that should match the record content.
    The key element<Integer> is the ANSIStyle value.
    The value <String> is a format template.
     */
    private final List<ANSIStyle.StyleWrapper> styles = new ArrayList<ANSIStyle.StyleWrapper>();

    public List<ANSIStyle.StyleWrapper> getStyles() {
        return styles;
    }

    public List<List<Object>> getRecords() {
        if (records.size() == 0 && baosOut.size() > 0) {
            BufferedReader bufferedReader;
            String line = null;
            bufferedReader = new BufferedReader(new StringReader(new String(baosOut.toByteArray())));
            // When the record size is 0, this means the records from the
            // command call are in the baosOut, read from the 'shell' of a
            // native Hadoop Shell Command.  We'll convert over to
            // records for better processing.
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    if (!line.startsWith("Found")) {
                        String[] parts = line.trim().split("\\s{1,}");
                        List<Object> record = Arrays.asList(parts);
                        records.add(record);
                    }
                }
            } catch (IOException ioe) {
                //
            }
        }
        return records;
    }

    public void addRecord(List<Object> record) {
        records.add(record);
    }

    public String[] getCommandArgs() {
        return commandArgs;
    }

    public String getCommand() {
        return StringUtils.join(commandArgs, " ");
    }

    public void setCommandArgs(String[] commandArgs) {
        this.commandArgs = commandArgs;
    }

    public void setCommandArgs(List<String> commandArgs) {
        String[] args = new String[commandArgs.size()];
        commandArgs.toArray(args);
        this.commandArgs = args;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isError() {
        if (code != 0)
            return true;
        else
            return false;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public PrintStream getOut() {
        return out;
    }

    public PrintStream getErr() {
        return err;
    }

    public CommandReturn(int code) {
        this.code = code;
    }

    public String getStyledReturn() {
        if (styles.size() > 0) {
            String outString = new String(this.baosOut.toByteArray());
            if ((outString != null && outString.length() > 0) || records.size() > 0) {
                StringBuilder sb = new StringBuilder();
                sb.append(outString);

                Iterator<List<Object>> rIter = records.iterator();
                while (rIter.hasNext()) {
                    List<Object> record = rIter.next();

                    Iterator<ANSIStyle.StyleWrapper> iter = styles.iterator();
                    Iterator<Object> iIter = record.iterator();
                    while (iIter.hasNext()) {
                        Object recItem = iIter.next();
                        if (iter.hasNext()) {
                            ANSIStyle.StyleWrapper styleWrapper = iter.next();
                            sb.append(ANSIStyle.style(recItem.toString(), styleWrapper));
                        } else {
                            sb.append(recItem.toString());
                        }
                        if (iIter.hasNext()) {
                            sb.append("\t");
                        }
                    }
                    if (rIter.hasNext()) {
                        sb.append("\n");
                    }
                }
                return sb.toString();
            } else {
                return null;
            }
        } else {
            return getReturn();
        }
    }

    public String getReturn() {
        String outString = new String(this.baosOut.toByteArray());
        if ((outString != null && outString.length() > 0) || records.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(outString);
            Iterator<List<Object>> rIter = records.iterator();
            while (rIter.hasNext()) {
                List<Object> record = rIter.next();
                Iterator<Object> iIter = record.iterator();
                while (iIter.hasNext()) {
                    Object recItem = iIter.next();
                        sb.append(recItem.toString());
                    if (iIter.hasNext()) {
                        sb.append("\t");
                    }
                }
                if (rIter.hasNext()) {
                    sb.append("\n");
                }
            }
            return sb.toString();
        } else {
            return null;
        }
    }

    public String getError() {
        if (error == null)
            error = new String(this.baosErr.toByteArray());
        return error;
    }

    public void setError(String errorMsg) {
        error = errorMsg;
    }
}
