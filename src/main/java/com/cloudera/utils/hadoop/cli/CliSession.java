/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.cli;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CliFsShell;

import java.io.IOException;

@Slf4j
@Getter
@Setter
public class CliSession {

    private CliFsShell shell;
    private FileSystemOrganizer fileSystemOrganizer;

    public void init(Configuration conf) {
        this.shell = new CliFsShell(conf);
        this.shell.init();
        this.fileSystemOrganizer = new FileSystemOrganizer();
        this.fileSystemOrganizer.init(conf);
    }
}
