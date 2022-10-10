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

package com.cloudera.utils.hadoop.hdfs.shell.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by streever on 2015-11-22.
 */

public class LocalCat extends HdfsCommand {
    
    public static final int LINE_COUNT = 10;
    
    public LocalCat(String name, Environment env) {
        super(name, env);
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {

        FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
        FileSystem lfs = lfss.getFileSystem();

        if(cmd.getArgs().length == 1){
//            Path path = new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[0]);
            Path path = new Path(lfss.getWorkingDirectory(), cmd.getArgs()[0]);
            BufferedReader reader = null;
            try {
                InputStream is = lfs.open(path);
                InputStreamReader isr = new InputStreamReader(is);
                reader = new BufferedReader(isr);
                String line = null;
                for(int i = 0; (line = reader.readLine()) != null;i++ ){
                    log(env, line);
                }
            }
            catch (IOException e) {
                log(env, "Error reading file '" + cmd.getArgs()[0] + "': " + e.getMessage());
            } finally{
                try {
                    if(reader != null){
                        reader.close();
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else{
//            usage();
        }
//        FSUtil.prompt(env);
        return commandReturn;
    }
    
    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        return opts;
    }
    
//    @Override
//    public Completer getCompleter() {
//        return new FileSystemNameCompleter(this.env, this.local);
//    }

}
