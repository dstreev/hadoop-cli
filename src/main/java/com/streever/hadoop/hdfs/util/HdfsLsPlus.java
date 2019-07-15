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
    private boolean invisible = false;
    private boolean addComment = false;
    private boolean showParent = false;
    private boolean test = false;
    // Used to shortcut 'test' and return when match located.
    private boolean testFound = false;
    private Pattern pattern = null;

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
//    public void setRecurse(Boolean recurse) {
//        this.recurse = recurse;
//    }

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

    //    public static boolean contains(int[] arr, int item) {
//        int index = Arrays.binarySearch(arr, item);
//        return index >= 0;
//    }
    public static boolean contains(PRINT_OPTION[] arr, PRINT_OPTION item) {
        return Arrays.stream(arr).anyMatch(item::equals);
    }


    private void writeItem(PathData item, FileStatus itemStatus, int level) {
        try {
            StringBuilder sb = new StringBuilder();

            boolean in = false;
            // Skip directories when PARENT is specified and is a directory.
            if (contains(print_options, PARENT) && itemStatus.isDirectory()) {
                return;
            }

            for (PRINT_OPTION option : print_options) {
                if (in && option != DATANODE_INFO)
                    sb.append(getSeparator());
                in = true;
                switch (option) {
                    case PERMISSIONS_SHORT:
                        if (itemStatus.isDirectory())
                            sb.append("1");
                        else
                            sb.append("0");
                        sb.append(itemStatus.getPermission().toOctal());
                        break;
                    case PERMISSIONS_LONG:
                        if (itemStatus.isDirectory()) {
                            sb.append("d");
                        }
                        sb.append(itemStatus.getPermission());
                        break;
                    case REPLICATION:
                        sb.append(itemStatus.getReplication());
                        break;
                    case USER:
                        sb.append(itemStatus.getOwner());
                        break;
                    case GROUP:
                        sb.append(itemStatus.getGroup());
                        break;
                    case SIZE:
                        sb.append(itemStatus.getLen());
                        break;
                    case BLOCK_SIZE:
                        sb.append(itemStatus.getBlockSize());
                        break;
                    case RATIO:
                        if (!itemStatus.isDirectory()) {
                            Double blockRatio = (double) itemStatus.getLen() / itemStatus.getBlockSize();
                            BigDecimal ratioBD = new BigDecimal(blockRatio, mc);
                            sb.append(ratioBD.toString());
                        }
                        break;
                    case MOD:
                        sb.append(df.format(new Date(itemStatus.getModificationTime())));
                        break;
                    case ACCESS:
                        sb.append(df.format(new Date(itemStatus.getAccessTime())));
                        break;
                    case PARENT:
                        sb.append(item.path.getParent().toString());
                        break;
                    case PATH:
                        sb.append(item.toString());
                        break;
                    case FILE:
                        if (!itemStatus.isDirectory())
                            sb.append(item.path.getName());
                        else
                            sb.append(".");
                        break;
                    case LEVEL:
                        sb.append(level);
                        break;
                }
            }
            if (!itemStatus.isDirectory() && Arrays.asList(print_options).contains(DATANODE_INFO)) {
                LocatedBlocks blocks = null;
                blocks = dfsClient.getLocatedBlocks(item.toString(), 0, Long.MAX_VALUE);
                if (blocks.getLocatedBlocks().size() == 0) {
                    // Happens on Zero Length Files.
                    StringBuilder sb1 = new StringBuilder(sb);
                    sb1.append(getSeparator());
                    sb1.append("none").append(getSeparator());
                    sb1.append("none").append(getSeparator());
                    sb1.append("na");
                    postItem(sb1.toString());

                } else {
                    for (LocatedBlock block : blocks.getLocatedBlocks()) {
                        DatanodeInfo[] datanodeInfo = block.getLocations();
//                    System.out.println("\tBlock: " + block.getBlock().getBlockName());

                        for (DatanodeInfo dni : datanodeInfo) {
//                        System.out.println(dni.getIpAddr() + " - " + dni.getHostName());
                            StringBuilder sb1 = new StringBuilder(sb);
                            sb1.append(getSeparator());
                            sb1.append(dni.getIpAddr()).append(getSeparator());
                            sb1.append(dni.getHostName()).append(getSeparator());
                            sb1.append(block.getBlock().getBlockName());
                            postItem(sb1.toString());
                        }
                    }
                }
            } else {
                postItem(sb.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void postItem(String line) {
        if (outFS != null) {
            try {
                if (isAddComment() && getComment() != null) {
                    StringBuilder sb = new StringBuilder(getComment());
                    sb.append(getSeparator());
                    sb.append(line);
                    outFS.write(sb.toString().getBytes());
                } else {
                    StringBuilder sb = new StringBuilder(line);
                    sb.append("\n");
                    outFS.write(sb.toString().getBytes());
                }
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
            if (isAddComment() && getComment() != null) {
                log(env, getComment() + getSeparator() + line);
            } else {
                log(env, line);
            }
        }
    }

    protected boolean doesMatch(PathData item, FileStatus itemStatus) {
        if (getPattern() != null) {
            StringBuilder sb = new StringBuilder();

            boolean in = false;
            // Skip directories when PARENT is specified and is a directory.
            if (contains(filter_options, PARENT) && itemStatus.isDirectory()) {
                return false;
            }

            for (PRINT_OPTION option : filter_options) {
                if (in && option != DATANODE_INFO)
                    sb.append(getSeparator());
                in = true;
                switch (option) {
                    case PERMISSIONS_SHORT:
                        if (itemStatus.isDirectory())
                            sb.append("1");
                        else
                            sb.append("0");
                        sb.append(itemStatus.getPermission().toOctal());
                        break;
                    case PERMISSIONS_LONG:
                        if (itemStatus.isDirectory()) {
                            sb.append("d");
                        }
                        sb.append(itemStatus.getPermission());
                        break;
                    case REPLICATION:
                        sb.append(itemStatus.getReplication());
                        break;
                    case USER:
                        sb.append(itemStatus.getOwner());
                        break;
                    case GROUP:
                        sb.append(itemStatus.getGroup());
                        break;
                    case SIZE:
                        sb.append(itemStatus.getLen());
                        break;
                    case BLOCK_SIZE:
                        sb.append(itemStatus.getBlockSize());
                        break;
                    case RATIO:
                        if (!itemStatus.isDirectory()) {
                            Double blockRatio = (double) itemStatus.getLen() / itemStatus.getBlockSize();
                            BigDecimal ratioBD = new BigDecimal(blockRatio, mc);
                            sb.append(ratioBD.toString());
                        }
                        break;
                    case MOD:
                        sb.append(df.format(new Date(itemStatus.getModificationTime())));
                        break;
                    case ACCESS:
                        sb.append(df.format(new Date(itemStatus.getAccessTime())));
                        break;
                    case PARENT:
                        sb.append(item.path.getParent().toString());
                        break;
                    case PATH:
                        sb.append(item.toString());
                        break;
                    case FILE:
                        if (!itemStatus.isDirectory())
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

    private boolean processPath(PathData path, int currentDepth) {
        boolean rtn = true;
        boolean subTestMatch = false;

        if (maxDepth == -1 || currentDepth <= (maxDepth + 1)) {

            FileStatus fileStatus = path.stat;
            try {
                String[] parts = null;
                String endPath = null;
                parts = fileStatus.getPath().toUri().getPath().split("/");
                if (parts.length > 0) {
                    endPath = parts[parts.length - 1];
                }
                boolean go = true;

//                if (endPath.startsWith(".") && !isInvisible()) {
//                    go = false;
//                    rtn = false;
//                }

                if (endPath == null || (!(endPath.startsWith(".") && isInvisible()))) {
                    if (fileStatus.isDirectory()) {
                        PathData[] pathDatas = new PathData[0];
                        try {
                            pathDatas = path.getDirectoryContents();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        for (PathData intPd : pathDatas) {
                            if ((intPd.stat.isDirectory() && isRecursive()) || !intPd.stat.isDirectory()) {
                                if (processPath(intPd, currentDepth + 1)) {
                                    if (isTest()) {
                                        // Test Found an Item, so we need to break
                                        subTestMatch = true;
                                        if (!intPd.stat.isDirectory()) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if (doesMatch(path, fileStatus)) {
                            if (!isTest()) {
                                writeItem(path, fileStatus, currentDepth);
                            } else {
                                setTestFound(true);
                                if (isAddComment()) {
                                    if (!isShowParent()) {
                                        writeItem(path, fileStatus, currentDepth);
                                    }
                                }
                            }
                        } else if (isTest() && (isAddComment() || isShowParent()) && subTestMatch) {
                            writeItem(path, fileStatus, currentDepth);
                            rtn = false;
                        } else {
                            rtn = false;
                        }

                    } else {
                        // Go through contents.
                        if (doesMatch(path, fileStatus)) {
                            if (!isTest()) {
                                writeItem(path, fileStatus, currentDepth);
                            } else {
                                setTestFound(true);
                                if (isAddComment() && !isShowParent()) {
                                    writeItem(path, fileStatus, currentDepth);
                                }
                            }
                        } else if (isTest()) {
                            rtn = false;
                        }
                    }
                } else {
                    rtn = false;
                }
            } catch (Throwable e) {
                // Happens when path doesn't exist.
                postItem("doesn't exist");
            }
        } else {
            logv(env, "Max Depth of: " + maxDepth + " Reached.  Sub-folder will not be traversed beyond this depth. Increase of set to -1 for unlimited depth");
            rtn = false;
        }
        return rtn;
    }

    @Override
    public CommandReturn execute(Environment environment, CommandLine cmd, ConsoleReader consoleReader) {
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
//            log(env, "Please connect first");
            return new CommandReturn(CODE_NOT_CONNECTED, "Connect First");
        }

        URI nnURI = fs.getUri();

        try {
            dfsClient = new DFSClient(nnURI, configuration);
        } catch (IOException e) {
//            e.printStackTrace();
            return new CommandReturn(CODE_CONNECTION_ISSUE, e.getMessage());
        }

        Option[] cmdOpts = cmd.getOptions();
        String[] cmdArgs = cmd.getArgs();

        if (cmd.hasOption("invert")) {
            setInvert(true);
        } else {
            setInvert(false);
        }

        if (cmd.hasOption("filter")) {
            String filter = cmd.getOptionValue("filter");
            setPattern(Pattern.compile(filter));
        } else {
            setPattern(null);
        }

        if (cmd.hasOption("filter-element")) {
            setFilterFormat(cmd.getOptionValue("filter-element"));
        } else {
            setFilterFormat("path"); // default
        }

        if (cmd.hasOption("format")) {
            setFormat(cmd.getOptionValue("format"));
        } else {
            setFormat(DEFAULT_FORMAT);
        }

        if (cmd.hasOption("comment")) {
            setComment(cmd.getOptionValue("comment"));
            setAddComment(true);
        } else {
            setAddComment(false);
        }

        if (cmd.hasOption("recursive")) {
            setRecursive(true);
        } else {
            setRecursive(false);
        }

        if (cmd.hasOption("test")) {
            setTest(true);
        } else {
            setTest(false);
        }

        if (cmd.hasOption("show-parent") && isTest()) {
            setShowParent(true);
        } else {
            setShowParent(false);
        }

        if (cmd.hasOption("maxDepth")) {
            setMaxDepth(Integer.parseInt(cmd.getOptionValue("maxDepth")));
            setRecursive(true);
        } else {
            setMaxDepth(DEFAULT_DEPTH);
        }

        if (cmd.hasOption("invisible")) {
            setInvisible(true);
        } else {
            setInvisible(false);
        }

        if (cmd.hasOption("separator")) {
            setSeparator(cmd.getOptionValue("separator"));
        } else {
            setSeparator(DEFAULT_SEPARATOR);
        }

        if (cmd.hasOption("newline")) {
            setNewLine(cmd.getOptionValue("newline"));
        } else {
            setNewLine(DEFAULT_NEWLINE);
        }

        String outputDir = null;
        String outputFile = null;

        String targetPath = null;
        if (cmdArgs.length > 0) {
            String pathIn = cmdArgs[0];
            targetPath = buildPath2(fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length()), pathIn);
        } else {
            targetPath = fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length());

        }

        if (cmd.hasOption("output")) {
            outputDir = buildPath2(fs.getWorkingDirectory().toString().substring(((String) env.getProperties().getProperty(Constants.HDFS_URL)).length()), cmd.getOptionValue("output"));
//            Date now = new Date();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HHmmss");
            String encodeTargetPath = targetPath.replace('/','_');
            outputFile = outputDir + "/" + df.format(new Date()) + "-" + encodeTargetPath;

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
//            e.printStackTrace();
            return new CommandReturn(CODE_PATH_ERROR, "Error in Path");
        }

        processPath(targetPathData, 1);

        if (outFS != null) {
            try {
                outFS.close();
            } catch (IOException e) {
//                e.printStackTrace();
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
                return new CommandReturn(CODE_NOT_FOUND, "");
            }
        }
        return cr;
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

//        opts.addOption("r", "recurse", false, "recurse (default false)");

        Option formatOption = Option.builder("f").required(false)
                .argName("output-format")
                .desc("Comma separated list of one or more: permissions_long,replication,user,group,size,block_size,ratio,mod,access,path,file,datanode_info (default all of the above)")
                .hasArg(true)
                .numberOfArgs(1)
                .valueSeparator(',')
                .longOpt("format")
                .build();
        opts.addOption(formatOption);

        Option recursiveOption = Option.builder("R").required(false)
                .argName("recursive")
                .desc("Process Path Recursively")
                .hasArg(false)
                .longOpt("recursive")
                .build();
        opts.addOption(recursiveOption);

        Option invisibleOption = Option.builder("i").required(false)
                .argName("invisible")
                .desc("Process Invisible Files/Directories")
                .hasArg(false)
                .longOpt("invisible")
                .build();
        opts.addOption(invisibleOption);


        Option depthOption = Option.builder("d").required(false)
                .argName("maxDepth")
                .desc("Depth of Recursion (default 5), use '-1' for unlimited")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("maxDepth")
                .build();
        opts.addOption(depthOption);

        Option filterOption = Option.builder("F").required(false)
                .argName("filter")
                .desc("Regex Filter of Content")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("filter")
                .build();
        opts.addOption(filterOption);

        Option filterElementOption = Option.builder("Fe").required(false)
                .argName("filter element")
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
                .argName("output directory")
                .desc("Output Directory (HDFS) (default System.out)")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("output")
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

        Option testOption = Option.builder("t").required(false)
                .argName("test")
                .desc("Test for existence")
                .hasArg(false)
                .longOpt("test")
                .build();
        opts.addOption(testOption);

        Option parentOption = Option.builder("sp").required(false)
                .argName("Show parent")
                .desc("For Test, show parent")
                .hasArg(false)
                .longOpt("show-parent")
                .build();
        opts.addOption(parentOption);

        return opts;
    }

}