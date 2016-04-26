package com.dstreev.hadoop.util;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;

/**
 * Created by dstreev on 2016-04-26.
 */
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
            // Newline
//            out.write("\n".getBytes());
            out.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

    }

//    public void write(byte[] bytes) {
//
//    }
}
