// Copyright (c) 2012 Health Market Science, Inc.

package com.instanceone.hdfs.shell;

import java.util.List;

import jline.Completor;

public class NoopCompletor implements Completor {

    public int complete(String buf, int cursor, List candidates) {
        // TODO Auto-generated method stub
        System.out.println("\nNoopCompletor: " + buf);
        for(Object c : candidates){
            System.out.println(c);
        }
        return -1;
    }

}
