package com.streever.hadoop.hdfs.util;

import com.streever.hadoop.hdfs.shell.command.Constants;
import com.streever.hadoop.hdfs.shell.command.Direction;
import com.streever.hadoop.hdfs.shell.command.HdfsAbstract;
import com.streever.tools.stemshell.Environment;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
    private static String DEFAULT_FORMAT = "permissions_long,replication,user,group,size,block_size,ratio,mod,access,path,datanode_info";

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
        PATH,
        DATANODE_INFO
    }

    // default
    private PRINT_OPTION[] print_options = new PRINT_OPTION[]{PERMISSIONS_LONG, PATH, REPLICATION,
            USER,
            GROUP,
            SIZE,
            BLOCK_SIZE,
            RATIO,
            MOD,
            ACCESS,
            DATANODE_INFO};

    private static int DEFAULT_DEPTH = 5;
    private static String DEFAULT_SEPARATOR = "\t";
    private static String DEFAULT_NEWLINE = "\n";
    private String separator = DEFAULT_SEPARATOR;
    private String newLine = DEFAULT_NEWLINE;
    private int currentDepth = 0;
    private boolean recursive = false;
    private boolean invisible = false;
    private boolean addComment = false;
    private String comment = null;
    private int maxDepth = DEFAULT_DEPTH;
    //    private Boolean recurse = Boolean.TRUE;
    private String format = DEFAULT_FORMAT;
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
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
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

    private void writeItem(PathData item, FileStatus itemStatus) {
        try {
            StringBuilder sb = new StringBuilder();

            boolean in = false;
            for (PRINT_OPTION option : print_options) {
                if (in && option != DATANODE_INFO)
                    sb.append(getSeparator());
                in = true;
                switch (option) {
                    case PERMISSIONS_SHORT:
                        sb.append(itemStatus.getPermission().toExtendedShort());
                        break;
                    case PERMISSIONS_LONG:
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
                    case PATH:
                        sb.append(item.toString());
                        break;
                }
            }
            if (!itemStatus.isDirectory() && Arrays.asList(print_options).contains(DATANODE_INFO)) {
                LocatedBlocks blocks = null;
                blocks = dfsClient.getLocatedBlocks(item.toString(), 0, Long.MAX_VALUE);
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
                        postItem(sb1.append(getNewLine()).toString());
                    }
                }
            } else {
                postItem(sb.append(getNewLine()).toString());
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
                    outFS.write(line.getBytes());
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
            log(env, getComment() + getSeparator() + line);
        }
    }

    private void processPath(PathData path) {
        //
        currentDepth++;
        if (maxDepth == -1 || currentDepth <= (maxDepth + 1)) {
            FileStatus fileStatus = path.stat;
            String[] parts = fileStatus.getPath().toUri().getPath().split("/");
            String endPath = parts[parts.length-1];
            boolean go = true;

            if (endPath.startsWith(".") && !isInvisible()) {
                go = false;
            }

            if (go) {
                if (fileStatus.isDirectory() && this.isRecursive()) {
                    writeItem(path, fileStatus);
                    PathData[] pathDatas = new PathData[0];
                    try {
                        pathDatas = path.getDirectoryContents();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    for (PathData intPd : pathDatas) {
                        processPath(intPd);
                    }
                } else {
                    // Go through contents.
                    writeItem(path, fileStatus);
                }
            }
        } else {
            logv(env, "Max Depth of: " + maxDepth + " Reached.  Sub-folder will not be traversed beyond this depth. Increase of set to -1 for unlimited depth");
        }
        currentDepth--;
    }

    @Override
    public void execute(Environment environment, CommandLine cmd, ConsoleReader consoleReader) {
        // reset counter.
        count = 0;
        logv(env, "Beginning 'lsp' collection.");

        // Get the Filesystem
        configuration = (Configuration) env.getValue(Constants.CFG);

        String hdfs_uri = (String) env.getProperty(Constants.HDFS_URL);

        fs = (FileSystem) env.getValue(Constants.HDFS);

        if (fs == null) {
            log(env, "Please connect first");
            return;
        }

        URI nnURI = fs.getUri();

        try {
            dfsClient = new DFSClient(nnURI, configuration);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        Option[] cmdOpts = cmd.getOptions();
        String[] cmdArgs = cmd.getArgs();

        if (cmd.hasOption("maxDepth")) {
            setMaxDepth(Integer.parseInt(cmd.getOptionValue("maxDepth")));
        } else {
            setMaxDepth(DEFAULT_DEPTH);
        }

        if (cmd.hasOption("format")) {
            setFormat(cmd.getOptionValue("format"));
        } else {
            setFormat(DEFAULT_FORMAT);
        }

        if (cmd.hasOption("comment")) {
            setComment(cmd.getOptionValue("comment"));
            setAddComment(true);
        }

        if (cmd.hasOption("recursive")) {
            setRecursive(true);
        } else {
            setRecursive(false);
        }

        if (cmd.hasOption("invisible")) {
            setInvisible(true);
        } else {
            setInvisible(false);
        }

        if (cmd.hasOption("separator")) {
            setSeparator(cmd.getOptionValue("separator"));
        }

        if (cmd.hasOption("newline")) {
            setNewLine(cmd.getOptionValue("newline"));
        }


        String outputFile = null;

        if (cmd.hasOption("output")) {
            outputFile = buildPath2(fs.getWorkingDirectory().toString().substring(((String) env.getProperty(Constants.HDFS_URL)).length()), cmd.getOptionValue("output"));
            Path pof = new Path(outputFile);
            try {
                if (fs.exists(pof))
                    fs.delete(pof, false);
                outFS = fs.create(pof);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        String targetPath = null;
        if (cmdArgs.length > 0) {
            String pathIn = cmdArgs[0];
            targetPath = buildPath2(fs.getWorkingDirectory().toString().substring(((String) env.getProperty(Constants.HDFS_URL)).length()), pathIn);
        } else {
            targetPath = fs.getWorkingDirectory().toString().substring(((String) env.getProperty(Constants.HDFS_URL)).length());

        }

        currentDepth = 0;

        PathData targetPathData = null;
        try {
            targetPathData = new PathData(targetPath, configuration);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        processPath(targetPathData);

        if (outFS != null)
            try {
                outFS.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                outFS = null;
            }

        logv(env,"'lsp' complete.");

    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

//        opts.addOption("r", "recurse", false, "recurse (default false)");

        Option formatOption = Option.builder("f").required(false)
                .argName("output-format")
                .desc("Comma separated list of one or more: permissions_long,replication,user,group,size,block_size,ratio,mod,access,path,datanode_info (default all of the above)")
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
                .argName("output")
                .desc("Output File (HDFS) (default System.out)")
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

        return opts;
    }

}