package com.streever.hadoop.hdfs.util;

import com.streever.hadoop.HadoopShell;
import com.streever.hadoop.hdfs.shell.command.Constants;
import com.streever.hadoop.hdfs.shell.command.HdfsAbstract;
import com.streever.tools.stemshell.Environment;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DFSClient;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

public class HdfsSource  extends HdfsAbstract {

    private FileSystem fs = null;

    private HadoopShell shell;
    private Configuration configuration = null;
    private DFSClient dfsClient = null;

    public HdfsSource(String name, Environment env, HadoopShell shell) {
        super(name, env);
        this.shell = shell;
    }

    @Override
    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {

        logv(env, "Beginning 'source' collection.");

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

        if (cmd.hasOption("lf")) {
            runSource(cmd.getOptionValue("lf"), reader);
        }

        logv(env,"'lsp' complete.");


    }

    private void runSource(String sourceFile, ConsoleReader reader) {
        this.shell.runFile(sourceFile,reader);
    }


    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        Option lfileOption = Option.builder("lf").required(false)
                .argName("source local file")
                .desc("local file to run")
                .hasArg(true)
                .numberOfArgs(1)
                .longOpt("localfile")
                .build();
        opts.addOption(lfileOption);

        // TODO: Add Distributed File Source
//        Option dfileOption = Option.builder("df").required(false)
//                .argName("source distributed file")
//                .desc("distributed file to run")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .longOpt("distributedfile")
//                .build();
//        opts.addOption(dfileOption);

//
//        Option commentOption = Option.builder("c").required(false)
//                .argName("comment")
//                .desc("Add comment to output")
//                .hasArg(true)
//                .numberOfArgs(1)
//                .longOpt("comment")
//                .build();
//        opts.addOption(commentOption);

        return opts;
    }

}
