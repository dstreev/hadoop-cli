// Copyright (c) 2012 Health Market Science, Inc.

package com.instanceone.hdfs.shell;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import jline.Completor;

public class ContextCompletor implements Completor {
    
    private Environment environment;
    
    public ContextCompletor(Environment env){
        this.environment = env;
    }

    public int complete(String buf, int cursor, List clist) {
//        System.err.println("\nTry to complete: " + buf);
        SortedSet  candidates = new TreeSet(this.environment.commandList());
        String start = (buf == null) ? "" : buf;

        SortedSet matches = candidates.tailSet(start);

        for (Iterator i = matches.iterator(); i.hasNext();) {
            String can = (String) i.next();

            if (!(can.startsWith(start))) {
                break;
            }

//            if (delimiter != null) {
//                int index = can.indexOf(delimiter, cursor);
//
//                if (index != -1) {
//                    can = can.substring(0, index + 1);
//                }
//            }

            clist.add(can);
        }

        if (clist.size() == 1) {
            //System.out.println("\nMatched: '" + clist.get(0) + "'");
            clist.set(0, ((String) clist.get(0)) + " ");
            
        }

        // the index of the completion is always from the beginning of
        // the buffer.
        return (clist.size() == 0) ? (-1) : 0;
    }

}
