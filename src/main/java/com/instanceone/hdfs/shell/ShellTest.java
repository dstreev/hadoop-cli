// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell;

import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;

public class ShellTest {

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

        @SuppressWarnings("unused")
        String line;
        while ((line = reader.readLine("hdfs-cli % ")) != null) {

        }
    }




}
