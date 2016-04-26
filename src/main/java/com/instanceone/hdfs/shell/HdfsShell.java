// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell;

import com.dstreev.hadoop.HadoopShell;

@Deprecated
public class HdfsShell {

    public static void main(String[] args) throws Exception {
        new HadoopShell().run(args);
    }

}
