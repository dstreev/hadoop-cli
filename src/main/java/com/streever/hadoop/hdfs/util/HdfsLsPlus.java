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

package com.streever.hadoop.hdfs.util;

import com.streever.hadoop.hdfs.shell.command.Constants;
import com.streever.hadoop.hdfs.shell.command.Direction;
import com.streever.hadoop.hdfs.shell.command.HdfsAbstract;
import com.streever.hadoop.hdfs.shell.command.PathBuilder;
import com.streever.tools.stemshell.Environment;
import com.streever.tools.stemshell.command.CommandReturn;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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

import static com.streever.hadoop.hdfs.util.HdfsLsPlus.PRINT_OPTION.*;

/**
 * Created by streever on 2016-02-15.
 * <p>
 * The intent here is to provide a means of querying the Namenode and
 * producing Metadata about the directory AND the files in it.
 */
public class HdfsLsPlus extends HdfsAbstract {

    private FileSystem fs = null;

    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    // TODO: Extended ACL's
    private static String DEFAULT_FORMAT = "permissions_long,replication,user,group,size,block_size,ratio,mod,access,path,datanode_info,level";
    private static String DEFAULT_FILTER_FORMAT = "path";

    enum PRINT_OPTION {
        PERMISSIONS_LONG,
        PERMISSIONS_SHORT,
        REPLICATION,
        USER,
        GROUP,
        SIZE,
        BLOCK_SIZE,
        RATIO,
        MOD,
        ACCESS,
        PARENT,
        PATH,
        FILE,
        DATANODE_INFO,
        LEVEL
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

    private static int DEFAULT_DEPTH = 5;
    private static String DEFAULT_SEPARATOR = "\t";
    private static String DEFAULT_NEWLINE = "\n";
    private String separator = DEFAULT_SEPARATOR;
    private String newLine = DEFAULT_NEWLINE;
    //    private int currentDepth = 0;
    private boolean recursive = false;
    private boolean relative = false;
    private boolean invisible = false;
    private boolean addComment = false;
    private boolean showParent = false;
    private boolean dirOnly = false;
    private boolean test = false;
    // Used to shortcut 'test' and return when match located.
    private boolean testFound = false;
    private Pattern pattern = null;

    private MessageFormat messageFormat = null;

    //    private PRINT_OPTION filterElement = PATH;
    private boolean invert = false;
    private String comment = null;
    private int maxDepth = DEFAULT_DEPTH;
    //    private Boolean recurse = Boolean.TRUE;
    private String format = DEFAULT_FORMAT;
    private String filterFormat = DEFAULT_FILTER_FORMAT;
    private Configuration configuration = null;
    private DFSClient dfsClient = null;
    private FSDataOutputStream outFS = null;
    private static MathContext mc = new MathContext(4, RoundingMode.HALF_UP);
    private int count = 0;
    private PathBuilder pathBuilder;

    public HdfsLsPlus(String name) {
        super(name);
    }

    public HdfsLsPlus(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public HdfsLsPlus(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public HdfsLsPlus(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public HdfsLsPlus(String name, Environment env) {
        super(name, env);
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
        this.format = format;
        String[] strOptions = this.format.split(",");
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
        this.filterFormat = format;
        String[] strOptions = this.filterFormat.split(",");
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


    private void writeItem(PathData item, int level) {

        List<String> output = new ArrayList<String>();

        // Don't write files when -do specified.
        if (item.stat.isFile() && isDirOnly())
            return;

        try {
            boolean in = false;
            // Skip directories when PARENT is specified and is a directory.
            if (contains(print_options, PARENT) && item.stat.isDirectory()) {
                return;
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
                        }
                        break;
                    case MOD:
                        output.add(df.format(new Date(item.stat.getModificationTime())));
                        break;
                    case ACCESS:
                        output.add(df.format(new Date(item.stat.getAccessTime())));
                        break;
                    case PARENT:
                        output.add(item.path.getParent().toString());
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
                }
            }
            if (!item.stat.isDirectory() && Arrays.asList(print_options).contains(DATANODE_INFO)) {
                LocatedBlocks blocks = null;
                blocks = dfsClient.getLocatedBlocks(item.toString(), 0, Long.MAX_VALUE);
                if (blocks.getLocatedBlocks().size() == 0) {
                    output.add("none");
                    output.add("none");
                    output.add("na");
                    postItem(output);

                } else {
                    for (LocatedBlock block : blocks.getLocatedBlocks()) {
                        DatanodeInfo[] datanodeInfo = block.getLocations();

                        for (DatanodeInfo dni : datanodeInfo) {
                            List<String> dno = new ArrayList<String>(output);
                            dno.add(dni.getIpAddr());
                            dno.add(dni.getHostName());
                            dno.add(block.getBlock().getBlockName());
                            postItem(dno);
                        }
                    }
                }
            } else {
                postItem(output);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getRecord(List<String> item) {
        Object[] itemArray = (Object[])item.toArray();
        String rtn = null;
        if (messageFormat != null) {
            rtn = messageFormat.format(itemArray);
        } else {
            StringBuilder sb = new StringBuilder();
            if (isAddComment() && getComment() != null) {
                sb.append(getComment());
                sb.append(getSeparator());
            }
            for (String i: item) {
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
                e.printStackTrace();
            }
            count++;
            if (count % 10 == 0)
                System.out.print(".");
            if (count % 1000 == 0)
                System.out.println();
            if (count % 10000 == 0)
                System.out.println("----------");
        } else {
            log(env, getRecord(item));
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
        if (path.startsWith(".")) { // removed this because it was actually a file.. || path.trim().length() == 0) {
            if (isInvisible()) {
                return true;
            } else
                return false;
        } else {
            return true;
        }
    }

    private boolean processPath(PathData path, PathData parent, int currentDepth) {
        boolean rtn = true;

        if (maxDepth == -1 || currentDepth <= (maxDepth + 1)) {

            if (env.isDebug()) {
                logd(env, "L:" + currentDepth + " - " + path.stat.getPath().toUri().getPath());
            }

            try {
                if (doesMatch(path)) {
                    boolean print = true;

                    if (isDirOnly() && !path.stat.isDirectory()) {
                        print = false;
                    }
                    if (isTest()) {
                        setTestFound(true);
                    }
                    if (isShowParent()) {
                        if (print) {
                            writeItem(parent, currentDepth - 1);
                            // Shortcut Recursion since we found a match and 'show parent'.
                            return false;
                        }
                    } else {
                        if (print) {
                            writeItem(path, currentDepth);
                        }
                    }
                }

                if (path.stat.isDirectory() && (isRecursive() || currentDepth == 1)) {
                    PathData[] pathDatas = new PathData[0];
                    try {
                        pathDatas = path.getDirectoryContents();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    for (PathData intPd : pathDatas) {
                        if (isShowParent()) {
                            if (!processPath(intPd, path, currentDepth + 1))
                                break;
                        } else {
                            processPath(intPd, path, currentDepth + 1);
                        }
                    }
                }
            } catch (Throwable e) {
                // Happens when path doesn't exist.
                List<String> j = new ArrayList<String>();
                j.add("doesn't exist");
                postItem(j);
            }
        } else {
            logv(env, "Max Depth of: " + maxDepth + " Reached.  Sub-folder will not be traversed beyond this depth. Increase of set to -1 for unlimited depth");
            rtn = false;
        }
        return rtn;
    }

    protected void processCommandLine(CommandLine commandLine) {
        super.processCommandLine(commandLine);

        if (commandLine.hasOption("invert")) {
            setInvert(true);
        } else {
            setInvert(false);
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

        if (commandLine.hasOption("test")) {
            setTest(true);
        } else {
            setTest(false);
        }

        if (commandLine.hasOption("show-parent")) {
            setShowParent(true);
            if (!commandLine.hasOption("filter")) {
                loge(env, "show-parent requires a 'filter'");
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
    }

    @Override
    public CommandReturn implementation(Environment environment, CommandLine cmd, ConsoleReader consoleReader) {
        // reset counter.
        count = 0;
        logv(env, "Beginning 'lsp' collection.");
        CommandReturn cr = CommandReturn.GOOD;

        // Check connect protocol
        if (!environment.getProperties().getProperty(Constants.CONNECT_PROTOCOL).equalsIgnoreCase(Constants.HDFS)) {
            loge(environment, "This function is only available when connecting via 'hdfs'");
            cr = new CommandReturn(-1, "Not available with this protocol");
            return cr;
        }

        // Reset
        setTestFound(false);

        // Get the Filesystem
        configuration = (Configuration) env.getValue(Constants.CFG);

        String hdfs_uri = (String) env.getProperties().getProperty(Constants.HDFS_URL);

        fs = (FileSystem) env.getValue(Constants.HDFS);

        if (fs == null) {
            return new CommandReturn(CODE_NOT_CONNECTED, "Connect First");
        }

        URI nnURI = fs.getUri();

        try {
            dfsClient = new DFSClient(nnURI, configuration);
        } catch (IOException e) {
            return new CommandReturn(CODE_CONNECTION_ISSUE, e.getMessage());
        }

        String[] cmdArgs = cmd.getArgs();

        processCommandLine(cmd);

        String outputDir = null;
        String outputFile = null;

        String targetPath = null;
        if (cmdArgs.length > 0) {
            String pathIn = cmdArgs[0];
            targetPath = pathBuilder.resolveFullPath(fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length()), pathIn);
        } else {
            targetPath = fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length());
        }

        if (cmd.hasOption("output-directory")) {
            outputDir = pathBuilder.resolveFullPath(fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length()), cmd.getOptionValue("output-directory"));
            outputFile = outputDir + "/" + UUID.randomUUID();

            Path pof = new Path(outputFile);
            try {
                if (fs.exists(pof))
                    fs.delete(pof, false);
                outFS = fs.create(pof);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        PathData targetPathData = null;
        try {
            targetPathData = new PathData(targetPath, configuration);
        } catch (IOException e) {
            return new CommandReturn(CODE_PATH_ERROR, "Error in Path");
        }

        processPath(targetPathData, null, 1);

        if (outFS != null) {
            try {
                outFS.close();
            } catch (IOException e) {
                return new CommandReturn(CODE_FS_CLOSE_ISSUE, e.getMessage());
            } finally {
                outFS = null;
            }
        }

        logv(env, "'lsp' complete.");

        if (isTest()) {
            if (isTestFound()) {
                return cr;
            } else {
                return new CommandReturn(CODE_NOT_FOUND, "Not Found");
            }
        }
        return cr;
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        Option formatOption = Option.builder("f").required(false)
                .argName("output format")
                .desc("Comma separated list of one or more: permissions_long,replication,user,group,size,block_size,ratio,mod,access,path,file,datanode_info (default all of the above)")
                .hasArg(true)
                .numberOfArgs(1)
                .valueSeparator(',')
                .longOpt("output-format")
                .build();
        opts.addOption(formatOption);

        Option recursiveOption = Option.builder("R").required(false)
                .argName("recursive")
                .desc("Process Path Recursively")
                .hasArg(false)
                .longOpt("recursive")
                .build();
        opts.addOption(recursiveOption);

        Option dirOnlyOption = Option.builder("do").required(false)
                .argName("directories only")
                .desc("Show Directories Only")
                .hasArg(false)
                .longOpt("dir-only")
                .build();
        opts.addOption(dirOnlyOption);

        Option relativeOutputOption = Option.builder("r").required(false)
                .argName("relative")
                .desc("Show Relative Path Output")
                .hasArg(false)
                .longOpt("relative")
                .build();
        opts.addOption(relativeOutputOption);


        Option invisibleOption = Option.builder("i").required(false)
                .argName("invisible")
                .desc("Process Invisible Files/Directories")
                .hasArg(false)
                .longOpt("invisible")
                .build();
        opts.addOption(invisibleOption);


        Option depthOption = Option.builder("d").required(false)
                .argName("max depth")
                .desc("Depth of Recursion (default 5), use '-1' for unlimited")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("max-depth")
                .build();
        opts.addOption(depthOption);

        Option filterOption = Option.builder("F").required(false)
                .argName("filter")
                .desc("Regex Filter of Content. Can be 'Quoted'")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("filter")
                .build();
        opts.addOption(filterOption);

        Option filterElementOption = Option.builder("Fe").required(false)
                .argName("Filter Element")
                .desc("Filter on 'element'.  One of '--format'")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("filter-element")
                .build();
        opts.addOption(filterElementOption);

        Option invertOption = Option.builder("v").required(false)
                .argName("invert")
                .desc("Invert Regex Filter of Content")
                .hasArg(false)
                .longOpt("invert")
                .build();
        opts.addOption(invertOption);

        Option sepOption = Option.builder("s").required(false)
                .argName("separator")
                .desc("Field Separator")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("separator")
                .build();
        opts.addOption(sepOption);

        Option newlineOption = Option.builder("n").required(false)
                .argName("newline")
                .desc("New Line")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("newline")
                .build();
        opts.addOption(newlineOption);


        Option outputOption = Option.builder("o").required(false)
                .argName("Output Directory")
                .desc("Output Directory (HDFS) (default System.out)")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("output-directory")
                .build();
        opts.addOption(outputOption);

        Option commentOption = Option.builder("c").required(false)
                .argName("comment")
                .desc("Add comment to output")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("comment")
                .build();
        opts.addOption(commentOption);

        Option messageFormatOption = Option.builder("mf").required(false)
                .argName("Message Format")
                .desc("Message Format function that uses the elements of the 'format' to populate a string message. See Java: https://docs.oracle.com/javase/8/docs/api/java/text/MessageFormat.html.")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("message-format")
                .build();
        opts.addOption(messageFormatOption);

        Option testOption = Option.builder("t").required(false)
                .argName("test")
                .desc("Test for existence")
                .hasArg(false)
                .longOpt("test")
                .build();
        opts.addOption(testOption);

        Option parentOption = Option.builder("sp").required(false)
                .argName("Show Parent")
                .desc("Shows the parent directory of the related matched 'filter'. When match is found, recursion into directory is aborted since parent has been identified.")
                .hasArg(false)
                .longOpt("show-parent")
                .build();
        opts.addOption(parentOption);


        return opts;
    }

}