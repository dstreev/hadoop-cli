// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell;

import java.io.PrintWriter;
import java.util.Arrays;

import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;



import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.instanceone.hdfs.shell.command.Env;
import com.instanceone.hdfs.shell.command.Exit;
import com.instanceone.hdfs.shell.command.HdfsCd;
import com.instanceone.hdfs.shell.command.HdfsConnect;
import com.instanceone.hdfs.shell.command.HdfsLs;
import com.instanceone.hdfs.shell.command.HdfsPut;
import com.instanceone.hdfs.shell.command.HdfsPwd;
import com.instanceone.hdfs.shell.command.HdfsRm;
import com.instanceone.hdfs.shell.command.Help;
import com.instanceone.hdfs.shell.command.LocalCwd;
import com.instanceone.hdfs.shell.command.LocalLs;
import com.instanceone.hdfs.shell.command.LocalPwd;

public class ShellTest {
    private static CommandLineParser parser = new PosixParser();

    public static void main(String[] args) throws Exception {
        
        
        
        StringsCompleter animal = new StringsCompleter("animal");
        StringsCompleter animals = new StringsCompleter("dog", "cat", "pig", "horse");
        ArgumentCompleter argCompleter = new ArgumentCompleter(animal, animals);
        
        StringsCompleter car = new StringsCompleter("car");
        StringsCompleter forDom = new StringsCompleter("foreign", "domestic");
        StringsCompleter cars = new StringsCompleter("honda", "subaru", "saab");
        ArgumentCompleter argCompleter2 = new ArgumentCompleter(car, forDom, cars);
        
        StringsCompleter file = new StringsCompleter("file");
        FileNameCompleter fnc = new FileNameCompleter();
        ArgumentCompleter fileCompleter = new ArgumentCompleter(file, fnc);
        
        
        AggregateCompleter completer = new AggregateCompleter(argCompleter, argCompleter2, fileCompleter);


        ConsoleReader reader = new ConsoleReader();
        reader.addCompleter(completer);

        String line;
        PrintWriter out = new PrintWriter(System.out);

        while ((line = reader.readLine("hdfs-cli % ")) != null) {

        }
    }




}
