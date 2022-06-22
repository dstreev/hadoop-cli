package com.cloudera.utils.hadoop;

import org.junit.Test;

import static org.junit.Assert.*;

public class HadoopShellTest {

    @Test
    public void FileTest01() {
        String[] args = new String[]{"-f", "test.1"};
        try {
            HadoopShell.main(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}