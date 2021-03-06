package com.streever.hadoop.hdfs.shell.command;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.hadoop.hdfs.util.FileSystemState;
import com.streever.hadoop.hdfs.util.HdfsLsPlus;
import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.CommandReturn;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.tools.AdminHelper;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HdfsAllowSnapshot extends HdfsAbstract {

    public HdfsAllowSnapshot(String name) {
        this(name, null, Direction.NONE);
    }

    @Override
    public String getDescription() {
        return "Allow HDFS Snapshots on a Directory";
    }

    public HdfsAllowSnapshot(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
        // Completer

        FileSystemNameCompleter fsc = new FileSystemNameCompleter(env);
        NullCompleter nullCompleter = new NullCompleter();
        Completer completer = new AggregateCompleter(fsc, nullCompleter);

        this.completer = completer;

    }

    public HdfsAllowSnapshot(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public HdfsAllowSnapshot(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public HdfsAllowSnapshot(String name, Environment env) {
        this(name, env, Direction.NONE);
    }


    @Override
    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        CommandReturn cr = commandReturn;
        try {
            // Check connect protocol
            if (env.getFileSystemOrganizer().isCurrentDefault()) {

                FileSystemState fss = env.getFileSystemOrganizer().getCurrentFileSystemState();

                DistributedFileSystem dfs = (DistributedFileSystem) fss.getFileSystem();//environment.getDistributedFileSystem();//getValue(Constants.HDFS);

                if (dfs == null) {
                    cr.setCode(CODE_NOT_CONNECTED);
                    cr.setError(("Connect first"));
                    err.println("Connect first");
                    return cr;
                }

                String[] cmdArgs = cmd.getArgs();

                String targetPath = null;
                if (cmdArgs.length > 0) {
                    String pathIn = cmdArgs[0];
                    targetPath = pathBuilder.resolveFullPath(fss.getWorkingDirectory().toString(), pathIn);
                } else {
                    targetPath = fss.getWorkingDirectory().toString();
                }

                Path path = new Path(targetPath);

                dfs.allowSnapshot(path);

            } else {
                loge(env, "This function is only available for the 'default' namespace");
                cr.setCode(-1);
                cr.setError("Not available for alternate namespace: " +
                        env.getFileSystemOrganizer().getCurrentFileSystemState().getNamespace());
                return cr;
            }

        } catch (RuntimeException rt) {
            loge(env, rt.getMessage() + " cmd:" + cmd.toString());
            rt.printStackTrace();
        } catch (IOException e) {
            loge(env, e.getMessage() + " cmd:" + cmd.toString());
            e.printStackTrace();
        }
        return cr;
    }

    public void allowSnapshot(String[] argv) throws IOException {
        Path p = new Path(argv[1]);
        if (env.getFileSystemOrganizer().getCurrentFileSystemState().getFileSystem() instanceof DistributedFileSystem) {
            final DistributedFileSystem dfs = (DistributedFileSystem) env.getFileSystemOrganizer().getCurrentFileSystemState().getFileSystem();
            try {
                dfs.allowSnapshot(p);
            } catch (SnapshotException e) {
                throw new RemoteException(e.getClass().getName(), e.getMessage());
            }
            System.out.println("Allowing snapshot on " + argv[1] + " succeeded");
        }
    }

}
