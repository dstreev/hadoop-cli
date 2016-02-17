package com.instanceone.hdfs.shell.command;

import com.dstreev.hdfs.shell.command.Direction;
import com.instanceone.stemshell.Environment;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.security.HadoopKerberosName;

/**
 * Created by dstreev on 2015-11-22.
 */

public class HdfsKrb extends HdfsCommand {

    public static final String USE_KERBEROS = "use.kerberos";
    public static final String HADOOP_AUTHENTICATION = "hadoop.security.authentication";
    public static final String HADOOP_AUTHORIZATION = "hadoop.security.authorization";
    public static final String HADOOP_KERBEROS_NN_PRINCIPAL = "dfs.namenode.kerberos.principal";
    public static final String KERBEROS = "kerberos";

    public HdfsKrb(String name, Environment env, Direction directionContext, int directives) {
        super(name,env,directionContext,directives);
//        Completer completer = new StringsCompleter("hdfs://localhost:8020/", "hdfs://hdfshost:8020/");
//        this.completer = completer;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            HadoopKerberosName.main(cmd.getArgs());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Completer getCompleter() {
        return this.completer;
    }


}
