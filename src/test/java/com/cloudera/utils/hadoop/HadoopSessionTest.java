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

package com.cloudera.utils.hadoop;

import com.cloudera.utils.hadoop.cli.HadoopSession;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;

public class HadoopSessionTest extends TestCase {

    private HadoopSession shell = null;

    public void setUp() throws Exception {
        super.setUp();
        shell = new HadoopSession();
        try {
            String[] api = {"-api"};
            shell.start(api);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void tearDown() throws Exception {
    }

    @Test
    public void test_001() {
        String[] commands = new String[] {
                "use default",
                "ls",
                "rm -r -f temp_api",
                "mkdir temp_api",
                "cd temp_api",
                "pwd",
                "cd ..",
                "pwd",
                "rm -r -f api_test",
                "mkdir -p api_test/subdir",
                "cd api_test",
                "lcd ~/.hadoop-cli/logs",
                "lls",
                "put hadoop-cli.log",
                "put hadoop-cli.log subdir",
                "ls -R",
                "pwd",
                "ls -R subdir",
                "cd ~",
                "pwd",
                "cd ~/api_test",
                "pwd",
                "ls",
                "lsp"
        };
        CommandReturn cr = null;
        for (String command : commands) {
            System.out.println("Command: " + command);
            cr = shell.processInput(command);
            if (cr.isError()) {
                assertFalse("Issue with command: " + command, Boolean.TRUE);
            }
            printCommandReturn(cr);
        }
    }

    @Test
    public void test_002() {
        String[] commands = new String[] {
                "use alt",
                "cd ~",
                "ls",
                "rm -r -f temp_api",
                "mkdir temp_api",
                "cd temp_api",
                "pwd",
                "cd ..",
                "pwd",
                "rm -r -f api_test",
                "mkdir -p api_test/subdir",
                "cd api_test",
                "lcd ~/.hadoop-cli/logs",
                "lls",
                "put hadoop-cli.log",
                "put hadoop-cli.log subdir",
                "ls -R",
                "pwd",
                "ls -R subdir",
                "cd ~",
                "pwd",
                "cd ~/api_test",
                "pwd",
                "ls",
                "lsp"
        };
        CommandReturn cr = null;
        for (String command : commands) {
            System.out.println("Command: " + command);
            cr = shell.processInput(command);
            printCommandReturn(cr);
            if (cr.isError()) {
                assertFalse("Issue with command: " + command, Boolean.TRUE);
            }
        }
    }

    @Test
    public void test_003() {
        String[] commands = new String[] {
                "use HDP50",
                "cd /user/dstreev/datasets/avro",
                "cp hdfs://HOME90/user/dstreev/datasets/avro/*.* ."
        };
        CommandReturn cr = null;
        for (String command : commands) {
            System.out.println("Command: " + command);
            cr = shell.processInput(command);
            printCommandReturn(cr);
            if (cr.isError()) {
                assertFalse("Issue with command: " + command, Boolean.TRUE);
            }
        }
    }

    protected void printCommandReturn(CommandReturn cr) {
        if (cr != null) {
            if (cr.isError()) {
                System.out.println("Error Code: " + cr.getCode());
                System.out.println("Error: " + cr.getError());
            } else {
                if (cr.getRecords() != null) {
                    List records = cr.getRecords();
                    for (Object record: records) {
                        System.out.println(record.toString());
                    }
                }
            }
        }
    }
}