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

package com.cloudera.utils.hadoop.hdfs.util;

import com.cloudera.utils.hadoop.hdfs.shell.command.PathBuilder;
import com.cloudera.utils.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hadoop.hdfs.shell.command.Direction;
import com.cloudera.utils.hadoop.hdfs.shell.command.HdfsAbstract;
import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cloudera.utils.hadoop.hdfs.util.HdfsLsPlus.PRINT_OPTION.*;

@Slf4j
public class HdfsLsPlus extends HdfsAbstract {

    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    // TODO: Extended ACL's
    private static final String DEFAULT_FORMAT = "permissions_long,replication,user,group,size,block_size,mod,path";
    private static String DEFAULT_FILTER_FORMAT = "path";

    enum PRINT_OPTION {
        PERMISSIONS_LONG(null, ANSIStyle.FG_BLUE),
        PERMISSIONS_SHORT(null, ANSIStyle.FG_BLUE),
        REPLICATION(null, ANSIStyle.FG_CYAN),
        USER(null, ANSIStyle.BOLD),
        GROUP(null, ANSIStyle.BOLD),
        SIZE(null, ANSIStyle.FG_RED),
        BLOCK_SIZE(null, null),
        RATIO(null, null),
        MOD(null, ANSIStyle.FG_BLUE),
        ACCESS(null, ANSIStyle.FG_BLUE),
        PARENT(null, ANSIStyle.FG_MAGENTA),
        PATH(null, ANSIStyle.FG_GREEN),
        FILE(null, ANSIStyle.FG_GREEN),
        DATANODE_INFO(null, null),
        LEVEL(null, null);

        private String format = null;
        private Integer[] style = null;

        PRINT_OPTION(String format, Integer... style) {
            this.format = format;
            this.style = style;
        }

        void addStyle(List<ANSIStyle.StyleWrapper> styles) {
            styles.add(new ANSIStyle.StyleWrapper(format, style));
        }

    }

    // default
    private PRINT_OPTION[] print_options =
            new PRINT_OPTION[]{PERMISSIONS_LONG,
                    PATH,
                    REPLICATION,
                    USER,
                    GROUP,
                    SIZE,
                    BLOCK_SIZE,
                    RATIO,
                    MOD,
                    ACCESS,
                    DATANODE_INFO,
                    LEVEL};

    private PRINT_OPTION[] filter_options =
            new PRINT_OPTION[]{
                    PATH};

    private static final int DEFAULT_DEPTH = 5;
    private static final String DEFAULT_SEPARATOR = "\t";
    private static final String DEFAULT_NEWLINE = "\n";
    private String separator = DEFAULT_SEPARATOR;
    private String newLine = DEFAULT_NEWLINE;
    //    private int currentDepth = 0;
    private boolean recursive = false;
    private boolean relative = false;
    private boolean invisible = false;
    private boolean addComment = false;
    private boolean showParent = false;
    private boolean dirOnly = false;
    private boolean self = false;
    private boolean test = false;
    // Used to shortcut 'test' and return when match located.
    private boolean testFound = false;
    private Pattern pattern = null;

    private MessageFormat messageFormat = null;

    //    private PRINT_OPTION filterElement = PATH;
    private boolean invert = false;
    private boolean count = false;
    private String comment = null;
    private int maxDepth = DEFAULT_DEPTH;
    private DFSClient dfsClient = null;
    private FSDataOutputStream outFS = null;
    private static final MathContext mc = new MathContext(4, RoundingMode.HALF_UP);
    private int processPosition = 0;
    private static final Pattern invisiblePattern = Pattern.compile("(.*/\\..*)|(.*/_orc_acid_version$)");
//    private static Pattern invisibleRegEx = Pattern.compile("(.*/\\..*)");
//    private PathBuilder pathBuilder;

    public HdfsLsPlus(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "List PLUS service for 'hdfs://' namespaces";
    }

    public HdfsLsPlus(String name, CliEnvironment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public HdfsLsPlus(String name, CliEnvironment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public HdfsLsPlus(String name, CliEnvironment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public HdfsLsPlus(String name, CliEnvironment env) {
        super(name, env);
    }

    @Override
    public Completer getCompleter() {
        FileSystemNameCompleter fsc = new FileSystemNameCompleter(env);
        NullCompleter nullCompleter = new NullCompleter();
        Completer completer = new AggregateCompleter(fsc, nullCompleter);

        return completer;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public boolean isRelative() {
        return relative;
    }

    public void setRelative(boolean relative) {
        this.relative = relative;
    }

    public boolean isInvisible() {
        return invisible;
    }

    public void setInvisible(boolean invisible) {
        this.invisible = invisible;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getNewLine() {
        return newLine;
    }

    public void setNewLine(String newLine) {
        this.newLine = newLine;
    }

    public boolean isAddComment() {
        return addComment;
    }

    public void setAddComment(boolean addComment) {
        this.addComment = addComment;
        if (!addComment)
            this.comment = null;
    }

    public boolean isSelf() {
        return self;
    }

    public void setSelf(boolean self) {
        this.self = self;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public MessageFormat getMessageFormat() {
        return messageFormat;
    }

    public void setMessageFormat(MessageFormat messageFormat) {
        this.messageFormat = messageFormat;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    public boolean isInvert() {
        return invert;
    }

    public void setInvert(boolean invert) {
        this.invert = invert;
    }

    public boolean isCount() {
        return count;
    }

    public void setCount(boolean count) {
        this.count = count;
    }

    public boolean isDirOnly() {
        return dirOnly;
    }

    public void setDirOnly(boolean dirOnly) {
        this.dirOnly = dirOnly;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public boolean isTest() {
        return test;
    }

    public void setTest(boolean test) {
        this.test = test;
    }

    public boolean isTestFound() {
        return testFound;
    }

    public void setTestFound(boolean testFound) {
        this.testFound = testFound;
    }

    public boolean isShowParent() {
        return showParent;
    }

    public void setShowParent(boolean showParent) {
        this.showParent = showParent;
    }

    public void setFormat(String format) {
        //    private Boolean recurse = Boolean.TRUE;
        String[] strOptions = format.split(",");
        List<PRINT_OPTION> options_list = new ArrayList<>();
        for (String strOption : strOptions) {
            PRINT_OPTION in = PRINT_OPTION.valueOf(strOption.toUpperCase());
            if (in != null) {
                options_list.add(in);
            }
        }
        print_options = new PRINT_OPTION[strOptions.length];
        print_options = options_list.toArray(print_options);
    }

    public void setFilterFormat(String format) {
        String[] strOptions = format.split(",");
        List<PRINT_OPTION> options_list = new ArrayList<>();
        for (String strOption : strOptions) {
            PRINT_OPTION in = PRINT_OPTION.valueOf(strOption.toUpperCase());
            if (in != null) {
                options_list.add(in);
            }
        }
        filter_options = new PRINT_OPTION[strOptions.length];
        filter_options = options_list.toArray(filter_options);

    }

    public static boolean contains(PRINT_OPTION[] arr, PRINT_OPTION item) {
        return Arrays.stream(arr).anyMatch(item::equals);
    }


    private List<Object> writeItem(PathData item, int level) {

        List<Object> output = new ArrayList<Object>();

        // Don't write files when -do specified.
        if (item.stat.isFile() && isDirOnly())
            return null;

        try {
            boolean in = false;
            // Skip directories when PARENT is specified and is a directory.
            if (contains(print_options, PARENT) && item.stat.isDirectory()) {
                return null;
            }

            logd(env, "L:" + level);

            for (PRINT_OPTION option : print_options) {
                in = true;
                switch (option) {
                    case PERMISSIONS_SHORT:
                        if (item.stat.isDirectory())
                            output.add("1" + Short.toString(item.stat.getPermission().toOctal()));
                        else
                            output.add("0" + Short.toString(item.stat.getPermission().toOctal()));
                        break;
                    case PERMISSIONS_LONG:
                        if (item.stat.isDirectory()) {
                            output.add("d" + item.stat.getPermission().toString());
                        } else {
                            output.add(item.stat.getPermission().toString());
                        }
                        break;
                    case REPLICATION:
                        output.add(Short.toString(item.stat.getReplication()));
                        break;
                    case USER:
                        output.add(item.stat.getOwner());
                        break;
                    case GROUP:
                        output.add(item.stat.getGroup());
                        break;
                    case SIZE:
                        output.add(Long.toString(item.stat.getLen()));
                        break;
                    case BLOCK_SIZE:
                        output.add(Long.toString(item.stat.getBlockSize()));
                        break;
                    case RATIO:
                        if (!item.stat.isDirectory()) {
                            Double blockRatio = (double) item.stat.getLen() / item.stat.getBlockSize();
                            BigDecimal ratioBD = new BigDecimal(blockRatio, mc);
                            output.add(ratioBD.toString());
                        } else {
                            output.add("\t");
                        }
                        break;
                    case MOD:
                        output.add(df.format(new Date(item.stat.getModificationTime())));
                        break;
                    case ACCESS:
                        output.add(df.format(new Date(item.stat.getAccessTime())));
                        break;
                    case PARENT:
//                        if (item.stat.getPath().getParent().toString())
                        output.add(item.stat.getPath().getParent().toString());
                        break;
                    case PATH:
                        if (!isRelative()) {
                            output.add(item.toString());
                        } else {
                            String[] parts = item.stat.getPath().toUri().getPath().split("/");
                            StringBuilder sbp = new StringBuilder();
                            for (int i = parts.length - level; i < parts.length; i++) {
                                if (parts[i].trim().length() == 0) {
                                    sbp.append(".");
                                } else {
                                    sbp.append(parts[i]);
                                    if (i < parts.length - 1 || item.stat.isDirectory())
                                        sbp.append("/");
                                }
                            }
                            output.add(sbp.toString());
                        }
                        break;
                    case FILE:
                        if (!item.stat.isDirectory()) {
                            output.add(item.path.getName());
                        } else {
                            output.add(".");
                        }
                        break;
                    case LEVEL:
                        output.add(Integer.toString(level));
                        break;
                    case DATANODE_INFO:
                        if (!item.stat.isDirectory()) {
                            LocatedBlocks blocks = null;
                            blocks = dfsClient.getLocatedBlocks(item.toString(), 0, Long.MAX_VALUE);
                            StringBuilder dnSb = new StringBuilder();
                            if (blocks.getLocatedBlocks().size() > 0) {
//                                output.add("none");
//                                output.add("none");
//                                output.add("na");
//                            } else {
                                for (LocatedBlock block : blocks.getLocatedBlocks()) {
                                    DatanodeInfo[] datanodeInfo = block.getLocations();
                                    dnSb.append("[");
                                    for (int i = 0; i < datanodeInfo.length; i++) {
                                        dnSb.append("{");
                                        dnSb.append(datanodeInfo[i].getIpAddr()).append(",");
                                        dnSb.append(datanodeInfo[i].getHostName()).append(",");
                                        dnSb.append(block.getBlock().getBlockName());
                                        dnSb.append("}");
                                        if (i < datanodeInfo.length - 1) {
                                            dnSb.append(",");
                                        }
                                    }

                                }
                            }
                            output.add(dnSb.toString());
                        } else {
                            output.add("\t");
                        }
                        break;
                }
            }
            // TODO: Need to Revisit the posting of Datanode Block Details.
//            if (!item.stat.isDirectory() && Arrays.asList(print_options).contains(DATANODE_INFO)) {
//                LocatedBlocks blocks = null;
//                blocks = dfsClient.getLocatedBlocks(item.toString(), 0, Long.MAX_VALUE);
//                if (blocks.getLocatedBlocks().size() == 0) {
//                    output.add("none");
//                    output.add("none");
//                    output.add("na");
////                    postItem(output);
//
//                } else {
//                    for (LocatedBlock block : blocks.getLocatedBlocks()) {
//                        DatanodeInfo[] datanodeInfo = block.getLocations();
//                        StringBuilder dnSb = new StringBuilder("[");
//                        for (int i = 0; i < datanodeInfo.length; i++) {
//                            dnSb.append("{");
//                            dnSb.append(datanodeInfo[i].getIpAddr()).append(",");
//                            dnSb.append(datanodeInfo[i].getHostName()).append(",");
//                            dnSb.append(block.getBlock().getBlockName());
//                            dnSb.append("}");
//                            if (i < datanodeInfo.length - 1) {
//                                dnSb.append(",");
//                            }
//                        }
//
////                        for (DatanodeInfo dni : datanodeInfo) {
////                            List<String> dno = new ArrayList<String>(output);
////                            dno.add(dni.getIpAddr());
////                            dno.add(dni.getHostName());
////                            dno.add(block.getBlock().getBlockName());
////                            postItem(dno);
////                        }
//                    }
//                }
//            } else {
////                postItem(output);
//            }

        } catch (IOException e) {
//            e.printStackTrace();
        }
        return output;
    }

    private String getRecord(List<String> item) {
        Object[] itemArray = (Object[]) item.toArray();
        String rtn = null;
        if (messageFormat != null) {
            rtn = messageFormat.format(itemArray);
        } else {
            StringBuilder sb = new StringBuilder();
            if (isAddComment() && getComment() != null) {
                sb.append(getComment());
                sb.append(getSeparator());
            }
            for (String i : item) {
                sb.append(i).append(getSeparator());
            }
            rtn = sb.toString();
        }
        return rtn;
    }

    private void postItem(List<String> item) {
        if (outFS != null) {
            try {
                outFS.write(getRecord(item).getBytes());
            } catch (IOException e) {
//                e.printStackTrace();
            }
            processPosition++;
            if (processPosition % 10 == 0)
                System.out.print(".");
            if (processPosition % 1000 == 0)
                System.out.println();
            if (processPosition % 10000 == 0)
                System.out.println("----------");
        } else {
            out.println(getRecord(item));
//            log(env, getRecord(item));
        }
    }

    protected boolean doesMatch(PathData item) {
        if (getPattern() != null) {
            StringBuilder sb = new StringBuilder();

            boolean in = false;
            // Skip directories when PARENT is specified and is a directory.
            if (contains(filter_options, PARENT) && item.stat.isDirectory()) {
                return false;
            }

            for (PRINT_OPTION option : filter_options) {
                if (in && option != DATANODE_INFO)
                    sb.append(getSeparator());
                in = true;
                switch (option) {
                    case PERMISSIONS_SHORT:
                        if (item.stat.isDirectory())
                            sb.append("1");
                        else
                            sb.append("0");
                        sb.append(item.stat.getPermission().toOctal());
                        break;
                    case PERMISSIONS_LONG:
                        if (item.stat.isDirectory()) {
                            sb.append("d");
                        }
                        sb.append(item.stat.getPermission());
                        break;
                    case REPLICATION:
                        sb.append(item.stat.getReplication());
                        break;
                    case USER:
                        sb.append(item.stat.getOwner());
                        break;
                    case GROUP:
                        sb.append(item.stat.getGroup());
                        break;
                    case SIZE:
                        sb.append(item.stat.getLen());
                        break;
                    case BLOCK_SIZE:
                        sb.append(item.stat.getBlockSize());
                        break;
                    case RATIO:
                        if (!item.stat.isDirectory()) {
                            Double blockRatio = (double) item.stat.getLen() / item.stat.getBlockSize();
                            BigDecimal ratioBD = new BigDecimal(blockRatio, mc);
                            sb.append(ratioBD.toString());
                        }
                        break;
                    case MOD:
                        sb.append(df.format(new Date(item.stat.getModificationTime())));
                        break;
                    case ACCESS:
                        sb.append(df.format(new Date(item.stat.getAccessTime())));
                        break;
                    case PARENT:
                        sb.append(item.path.getParent().toString());
                        break;
                    case PATH:
                        sb.append(item.toString());
                        break;
                    case FILE:
                        if (!item.stat.isDirectory())
                            sb.append(item.path.getName());
                        else
                            sb.append(".");
                        break;
                }
            }
            String check = sb.toString();
            Matcher matcher = getPattern().matcher(check);
            if (isInvert()) {
                return !matcher.matches();
            } else {
                return matcher.matches();
            }
        } else {
            return true;
        }
    }

    protected boolean checkVisible(String path) {
        // Filter out invisibles
        Matcher invisible = invisiblePattern.matcher(path);
        if (invisible.matches()) { // removed this because it was actually a file.. || path.trim().length() == 0) {
            if (isInvisible()) {
                return true;
            } else
                return false;
        } else {
            return true;
        }
    }

    private boolean processPath(PathData path, PathData parent, int currentDepth, CommandReturn commandReturn) {
        boolean rtn = true;

        if (maxDepth == -1 || currentDepth <= (maxDepth + 1)) {

            if (env.isDebug()) {
                logd(getEnv(), "L:" + currentDepth + " - " + path.stat.getPath().toUri().getPath());
            }

            try {
                if (doesMatch(path) && checkVisible(path.toString())) {
                    boolean print = true;

                    if (isDirOnly() && !path.stat.isDirectory()) {
                        print = false;
                    }
                    if (isTest()) {
                        setTestFound(true);
                    }
                    if (isShowParent()) {
                        if (print) {
                            List<Object> output = writeItem(parent, currentDepth - 1);
                            if (output != null) {
                                commandReturn.addRecord(output);
                            }
                            // Shortcut Recursion since we found a match and 'show parent'.
                            return false;
                        }
                    } else {
                        if (print) {
                            if (isCount()) {
                                addToCounter(commandReturn, path);
                            } else {
                                List<Object> output = writeItem(path, currentDepth);
                                if (output != null) {
                                    commandReturn.addRecord(output);
                                }
                            }
                        }
                    }
                }

                if (path.stat.isDirectory() && (isRecursive() || currentDepth == 1) && !isSelf()) {
                    PathData[] pathDatas = new PathData[0];
                    try {
                        pathDatas = path.getDirectoryContents();
                    } catch (IOException e) {
//                        e.printStackTrace();
                    }

                    for (PathData intPd : pathDatas) {
                        if (isShowParent()) {
                            if (!processPath(intPd, path, currentDepth + 1, commandReturn))
                                break;
                        } else {
                            processPath(intPd, path, currentDepth + 1, commandReturn);
                        }
                    }
                }
            } catch (Throwable e) {
                // Happens when path doesn't exist.
                List<String> j = new ArrayList<String>();
                j.add("doesn't exist");
                commandReturn.setCode(CODE_PATH_ERROR);
                commandReturn.setError("Doesn't exists");
                postItem(j);
            }
        } else {
            logv(env, "Max Depth of: " + maxDepth + " Reached.  Sub-folder will not be traversed beyond this depth. Increase of set to -1 for unlimited depth");
            rtn = false;
        }
        return rtn;
    }

    private void addToCounter(CommandReturn commandReturn, PathData pathData) {
        List<Object> countRecord = null;
        if (commandReturn.getRecords().size() == 1) {
            // Get first record.
            countRecord = commandReturn.getRecords().get(0);
        } else {
            countRecord = new ArrayList<Object>();
            countRecord.add(0);
            countRecord.add(0);
            countRecord.add(0l);
            countRecord.add(pathData.path.toString());
            commandReturn.getRecords().add(countRecord);
        }
        if (pathData.stat.isDirectory()) {
            int dir = (Integer) countRecord.get(0);
            dir += 1;
            countRecord.set(0, dir);
        } else {
            int file = (Integer) countRecord.get(1);
            file += 1;
            countRecord.set(1, file);
            long size = (Long) countRecord.get(2);
            size += pathData.stat.getLen();
            countRecord.set(2, size);
        }
    }

    protected void processCommandLine(CommandLine commandLine) {
        super.processCommandLine(commandLine);

        if (commandLine.hasOption("invert")) {
            setInvert(true);
        } else {
            setInvert(false);
        }

        if (commandLine.hasOption("c")) {
            setCount(true);
            // Allow recursion till path end.
            setMaxDepth(-1);
            // Set Recursion to allow build of counts
            setRecursive(true);
        } else {
            setCount(false);
        }

        if (commandLine.hasOption("filter")) {
            String filter = commandLine.getOptionValue("filter");
            String adjustedFilter = null;
            if (filter.startsWith("\"") || filter.startsWith("'")) {
                if (filter.endsWith("\"") || filter.endsWith("'")) {
                    // Strip quotes.
                    adjustedFilter = filter.substring(1, filter.length() - 1);
                }
            }
            if (adjustedFilter == null)
                adjustedFilter = filter;
            logv(env, "Adjusted Filter: " + adjustedFilter);
            setPattern(Pattern.compile(adjustedFilter));
        } else {
            setPattern(null);
        }

        if (commandLine.hasOption("dir-only")) {
            setDirOnly(true);
        } else {
            setDirOnly(false);
        }

        if (commandLine.hasOption("relative")) {
            setRelative(true);
        } else {
            setRelative(false);
        }

        if (commandLine.hasOption("filter-element")) {
            setFilterFormat(commandLine.getOptionValue("filter-element"));
        } else {
            setFilterFormat("path"); // default
        }

        if (commandLine.hasOption("output-format")) {
            setFormat(commandLine.getOptionValue("output-format"));
        } else {
            setFormat(DEFAULT_FORMAT);
        }

        if (commandLine.hasOption("comment")) {
            setComment(commandLine.getOptionValue("comment"));
            setAddComment(true);
        } else {
            setAddComment(false);
        }

        if (commandLine.hasOption("message-format")) {
            messageFormat = new MessageFormat(commandLine.getOptionValue("message-format"));
            setAddComment(false);
        } else {
            messageFormat = null;
        }

        if (commandLine.hasOption("recursive")) {
            setRecursive(true);
            setMaxDepth(DEFAULT_DEPTH);
        } else {
            setRecursive(false);
            setMaxDepth(1);
        }

        if (commandLine.hasOption("c")) {
            setCount(true);
            // Allow recursion till path end.
            setMaxDepth(-1);
            // Set Recursion to allow build of counts
            setRecursive(true);
        } else {
            setCount(false);
        }

        if (commandLine.hasOption("test")) {
            setTest(true);
        } else {
            setTest(false);
        }

        if (commandLine.hasOption("show-parent")) {
            setShowParent(true);
            if (!commandLine.hasOption("filter")) {
                err.println("show-parent requires a 'filter'");
//                loge(env, "show-parent requires a 'filter'");
            }
        } else {
            setShowParent(false);
        }

        if (commandLine.hasOption("max-depth")) {
            setMaxDepth(Integer.parseInt(commandLine.getOptionValue("max-depth")));
            setRecursive(true);
        } else {
            // Don't reset Recursive if already set.
            if (!isRecursive())
                setMaxDepth(1);
        }

        if (commandLine.hasOption("invisible")) {
            setInvisible(true);
        } else {
            setInvisible(false);
        }

        if (commandLine.hasOption("separator")) {
            setSeparator(commandLine.getOptionValue("separator"));
        } else {
            setSeparator(DEFAULT_SEPARATOR);
        }

        if (commandLine.hasOption("newline")) {
            setNewLine(commandLine.getOptionValue("newline"));
        } else {
            setNewLine(DEFAULT_NEWLINE);
        }

        if (commandLine.hasOption("self")) {
            setSelf(true);
            setRecursive(false);
            setInvisible(true);
        } else {
            setSelf(false);
        }

    }

    @Override
    public CommandReturn implementation(CliEnvironment cliEnvironment, CommandLine cmd, CommandReturn commandReturn) {
        // reset counter.
        processPosition = 0;
        logv(cliEnvironment, "Beginning 'lsp' collection.");
        CommandReturn cr = commandReturn;
        FileSystemOrganizer fso = cliEnvironment.getFileSystemOrganizer();
        try {
            FileSystemState fss = fso.getCurrentFileSystemState();
            // Check connect protocol
            // TODO: NEED TO ACCOUNT FOR THE NONE DEFAULT HDFS...
            //   Right now, only looks at the defaultFS.
            if (fss.getProtocol().equalsIgnoreCase("hdfs://")
                    // TODO: doesn't currently work with ofs.
//                    || fss.getProtocol().equalsIgnoreCase("ofs://")
            ) {
                setTestFound(false);

                // Get the Filesystem
                Configuration configuration = cliEnvironment.getHadoopConfig();

                FileSystem fs = fss.getFileSystem();//environment.getDistributedFileSystem();//getValue(Constants.HDFS);

                if (fs == null) {
                    cr.setCode(CODE_NOT_CONNECTED);
                    cr.setError(("Connect first"));
                    err.println("Connect first");
                    return cr;
                }

                URI nnURI = fs.getUri();

                try {
                    dfsClient = new DFSClient(nnURI, configuration);
                } catch (IOException e) {
                    cr.setCode(CODE_CONNECTION_ISSUE);
                    cr.setError(e.getMessage());
                    err.println(e.getMessage());
                    return cr;
                }
                Option[] cmdOpts = cmd.getOptions();

                List<String> cmdParts = new ArrayList<String>();
                cmdParts.add("lsp");
                for (Option option : cmdOpts) {
                    cmdParts.add("-" + option.getOpt());
                }

                String[] cmdArgs = cmd.getArgs();

                cmdParts.addAll(cmd.getArgList());

                cr.setCommandArgs(cmdParts);

                processCommandLine(cmd);

                for (PRINT_OPTION po : print_options) {

                    po.addStyle(cr.getStyles());
                }

                String outputDir = null;
                String outputFile = null;

                String targetPath = null;
                if (cmdArgs.length > 0) {
                    String pathIn = cmdArgs[0];
                    targetPath = PathBuilder.resolveFullPath(fss.getWorkingDirectory().toString(), pathIn);
                } else {
                    targetPath = fss.getWorkingDirectory().toString();
                }

                if (cmd.hasOption("output-directory")) {
                    outputDir = PathBuilder.
                            resolveFullPath(fss.getWorkingDirectory().toString(), cmd.getOptionValue("output-directory"));
                    outputFile = outputDir + "/" + UUID.randomUUID();

                    Path pof = new Path(outputFile);
                    try {
                        if (fs.exists(pof))
                            fs.delete(pof, false);
                        outFS = fs.create(pof);
                    } catch (IOException e) {
                        log.error("Issue with output file: {}", outputFile, e);
                    }
                }

                PathData targetPathData = null;
                try {
                    targetPathData = new PathData(targetPath, configuration);
                    cr.setPath(targetPathData.path.toString());
                } catch (IOException e) {
                    cr.setCode(CODE_PATH_ERROR);
                    cr.setError("No such file or directory: " + targetPath);
                    return cr;
                }

                processPath(targetPathData, null, 1, cr);

                if (outFS != null) {
                    try {
                        outFS.close();
                    } catch (IOException e) {
                        cr.setCode(CODE_FS_CLOSE_ISSUE);
                        cr.setError(e.getMessage());
                        return cr;
                    } finally {
                        outFS = null;
                    }
                }

                logv(cliEnvironment, "'lsp' complete.");

                if (isTest()) {
                    if (!isTestFound()) {
                        cr.setCode(CODE_NOT_FOUND);
                        cr.setError("Not Found");
                        cr.getErr().print("Not Found");
                    }
                }
            } else {
                log.error("This function is only available when connecting via 'hdfs'");
                cr.setCode(-1);
                cr.setError("Not available with this protocol");
                return cr;
            }

        } catch (RuntimeException rt) {
            log.error("{} cmd: {}", rt.getMessage(), cmd.toString());
        }
        return cr;
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        Option formatOption = new Option("f", "output-format", true,
                "Comma separated list of one or more: " +
                        "permissions_long,replication,user,group,size,block_size,ratio,mod," +
                        "access,path,file,datanode_info (default all of the above)");
        formatOption.setRequired(false);
        //        Option formatOption = Option.builder("f").required(false)
        //                .argName("output format")
        //                .desc("Comma separated list of one or more: permissions_long,replication,user,group,size,block_size,ratio,mod,access,path,file,datanode_info (default all of the above)")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .valueSeparator(',')
        //                .longOpt("output-format")
        //                .build();
        opts.addOption(formatOption);

        Option recursiveOption = new Option("R", "recursive", false, "Process Path Recursively");
        recursiveOption.setRequired(false);
        //        Option recursiveOption = Option.builder("R").required(false)
        //                .argName("recursive")
        //                .desc("Process Path Recursively")
        //                .hasArg(false)
        //                .longOpt("recursive")
        //                .build();
        opts.addOption(recursiveOption);

        Option dirOnlyOption = new Option("do", "dir-only", false, "Show Directories Only");
        dirOnlyOption.setRequired(false);
        //        Option dirOnlyOption = Option.builder("do").required(false)
        //                .argName("directories only")
        //                .desc("Show Directories Only")
        //                .hasArg(false)
        //                .longOpt("dir-only")
        //                .build();
        opts.addOption(dirOnlyOption);

        Option relativeOutputOption = new Option("r", "relative", false, "Show Relative Path Output");
        relativeOutputOption.setRequired(false);
        //        Option relativeOutputOption = Option.builder("r").required(false)
        //                .argName("relative")
        //                .desc("Show Relative Path Output")
        //                .hasArg(false)
        //                .longOpt("relative")
        //                .build();
        opts.addOption(relativeOutputOption);


        Option invisibleOption = new Option("i", "invisible", false,
                "Process Invisible Files/Directories matching " +
                        "regex \"(.*/\\..*)|(.*/_orc_acid_version$)\"");
        invisibleOption.setRequired(false);
        //        Option invisibleOption = Option.builder("i").required(false)
        //                .argName("invisible")
        //                .desc("Process Invisible Files/Directories")
        //                .hasArg(false)
        //                .longOpt("invisible")
        //                .build();
        opts.addOption(invisibleOption);


        Option depthOption = new Option("d", "max-depth", true,
                "Depth of Recursion (default 5), use '-1' for unlimited");
        depthOption.setRequired(false);
        //        Option depthOption = Option.builder("d").required(false)
        //                .argName("max depth")
        //                .desc("Depth of Recursion (default 5), use '-1' for unlimited")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("max-depth")
        //                .build();
        opts.addOption(depthOption);

        Option filterOption = new Option("F", "filter", true,
                "Regex Filter of Content. Can be 'Quoted'");
        filterOption.setRequired(false);
        //        Option filterOption = Option.builder("F").required(false)
        //                .argName("filter")
        //                .desc("Regex Filter of Content. Can be 'Quoted'")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("filter")
        //                .build();
        opts.addOption(filterOption);

        Option filterElementOption = new Option("Fe", "filter-element", true,
                "Filter on 'element'.  One of '--format'");
        filterElementOption.setRequired(false);
        //        Option filterElementOption = Option.builder("Fe").required(false)
        //                .argName("Filter Element")
        //                .desc("Filter on 'element'.  One of '--format'")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("filter-element")
        //                .build();
        opts.addOption(filterElementOption);

        Option invertOption = new Option("v", "invert", false,
                "Invert Regex Filter of Content");
        invertOption.setRequired(false);
        //        Option invertOption = Option.builder("v").required(false)
        //                .argName("invert")
        //                .desc("Invert Regex Filter of Content")
        //                .hasArg(false)
        //                .longOpt("invert")
        //                .build();
        opts.addOption(invertOption);

        Option sepOption = new Option("s", "separator", true, "Field Separator");
        sepOption.setRequired(false);
        //        Option sepOption = Option.builder("s").required(false)
        //                .argName("separator")
        //                .desc("Field Separator")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("separator")
        //                .build();
        opts.addOption(sepOption);

        Option newlineOption = new Option("n", "newline", true, "New Line");
        newlineOption.setRequired(false);
        //        Option newlineOption = Option.builder("n").required(false)
        //                .argName("newline")
        //                .desc("New Line")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("newline")
        //                .build();
        opts.addOption(newlineOption);


        Option outputOption = new Option("o", "output-directory", true,
                "Output Directory (HDFS) (default System.out)");
        outputOption.setRequired(false);
        //        Option outputOption = Option.builder("o").required(false)
        //                .argName("Output Directory")
        //                .desc("Output Directory (HDFS) (default System.out)")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("output-directory")
        //                .build();
        opts.addOption(outputOption);

        Option commentOption = new Option("c", "comment", true, "Add comment to output");
        commentOption.setRequired(false);
        //        Option commentOption = Option.builder("c").required(false)
        //                .argName("comment")
        //                .desc("Add comment to output")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("comment")
        //                .build();
        opts.addOption(commentOption);

        Option messageFormatOption = new Option("mf", "message-format", true,
                "Message Format function that uses the elements of the 'format' to populate a string message." +
                        " See Java: https://docs.oracle.com/javase/8/docs/api/java/text/MessageFormat.html.");
        messageFormatOption.setRequired(false);
        //        Option messageFormatOption = Option.builder("mf").required(false)
        //                .argName("Message Format")
        //                .desc("Message Format function that uses the elements of the 'format' to populate a string message. See Java: https://docs.oracle.com/javase/8/docs/api/java/text/MessageFormat.html.")
        //                .hasArg(true)
        //                .numberOfArgs(1)
        //                .longOpt("message-format")
        //                .build();
        opts.addOption(messageFormatOption);

        Option testOption = new Option("t", "test", false,
                "Test for existence");
        testOption.setRequired(false);
        //        Option testOption = Option.builder("t").required(false)
        //                .argName("test")
        //                .desc("Test for existence")
        //                .hasArg(false)
        //                .longOpt("test")
        //                .build();
        opts.addOption(testOption);

        Option parentOption = new Option("sp", "show-parent", false,
                "Shows the parent directory of the related matched 'filter'. " +
                        "When match is found, recursion into directory is aborted since parent has been identified.");
        parentOption.setRequired(false);
        //        Option parentOption = Option.builder("sp").required(false)
        //                .argName("Show Parent")
        //                .desc("Shows the parent directory of the related matched 'filter'. When match is found, recursion into directory is aborted since parent has been identified.")
        //                .hasArg(false)
        //                .longOpt("show-parent")
        //                .build();
        opts.addOption(parentOption);

        Option countOption = new Option("c", "count", false,
                "Count");
        countOption.setRequired(false);
        //        Option parentOption = Option.builder("sp").required(false)
        //                .argName("Show Parent")
        //                .desc("Shows the parent directory of the related matched 'filter'. When match is found, recursion into directory is aborted since parent has been identified.")
        //                .hasArg(false)
        //                .longOpt("show-parent")
        //                .build();
        opts.addOption(countOption);

        Option selfOption = new Option("self", "self", false,
                "Only review path in command");
        selfOption.setRequired(false);
        //        Option parentOption = Option.builder("sp").required(false)
        //                .argName("Show Parent")
        //                .desc("Shows the parent directory of the related matched 'filter'. When match is found, recursion into directory is aborted since parent has been identified.")
        //                .hasArg(false)
        //                .longOpt("show-parent")
        //                .build();
        opts.addOption(selfOption);

        return opts;
    }

}