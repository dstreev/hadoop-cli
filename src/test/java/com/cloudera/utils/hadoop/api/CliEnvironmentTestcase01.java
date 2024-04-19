/*
 * Copyright (c) 2022-2024. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.api;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.cli.DisabledException;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.cloudera.utils.hadoop.HadoopCliApp.class)
@ActiveProfiles("test")
@Slf4j
public class CliEnvironmentTestcase01 {

    private CliEnvironment cliEnvironment;

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Test
    public void test_001() {
        String[] commands = new String[]{
                "use default",
                "cd /user/dstreev",
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
        runThreadedCommandList(commands, 5);
    }

    @Test
    public void test_002() {
        String[] commands = new String[]{
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
        runCommandList(commands);
    }

    @Test
    public void test_003() {
        String[] commands = new String[]{
                "use HDP50",
                "cd /user/dstreev/datasets/avro",
                "cp hdfs://HOME90/user/dstreev/datasets/avro/*.* ."
        };
        runCommandList(commands);
    }

    protected void runThreadedCommandList(String[]commands, int threadCount) {
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    runCommandList(commands);
                }
            });
            t.start();
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            Thread.sleep(threadCount * 2000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void runCommandList(String[] commands) {
        CommandReturn cr = null;
        for (String command : commands) {
            log.info("Command: {}", command);
            try {
                cr = cliEnvironment.processInput(command);
                if (cr.isError()) {
                    assertFalse("Issue with command: " + command, Boolean.TRUE);
                }
                printCommandReturn(cr);
            } catch (DisabledException e) {
                log.error("CLI Disabled", e);
                fail();
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
                    for (Object record : records) {
                        System.out.println(record.toString());
                    }
                }
            }
        }
    }
}