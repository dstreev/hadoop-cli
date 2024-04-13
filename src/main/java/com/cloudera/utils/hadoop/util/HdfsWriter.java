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

package com.cloudera.utils.hadoop.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;

/**
 * Created by streever on 2016-04-26.
 */
@Slf4j
public class HdfsWriter {

    private DistributedFileSystem fs = null;
    private String pathStr = null;

    public HdfsWriter(DistributedFileSystem fs, String path) {
        this.fs = fs;
        this.pathStr = path;
    }

    public void append(byte[] in) {
        Path path = new Path(pathStr);
        FSDataOutputStream out = null;
        try {
            if (fs.exists(path)) {
                out = fs.append(path);
            } else {
                out = fs.create(path);
            }
            out.write(in);
            out.flush();

        } catch (IOException ioe) {
            log.error("Error writing to HDFS: " + pathStr, ioe);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    log.error("Error closing HDFS output stream: " + pathStr, e);
                }
            }
        }

    }

}
