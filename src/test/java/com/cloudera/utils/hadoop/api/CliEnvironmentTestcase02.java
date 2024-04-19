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

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.cloudera.utils.hadoop.HadoopCliApp.class)
@ActiveProfiles("test")
@Slf4j
public class CliEnvironmentTestcase02 {

    private CliEnvironment cliEnvironment;

    @Autowired
    public void setCliEnvironment(CliEnvironment cliEnvironment) {
        this.cliEnvironment = cliEnvironment;
    }

    @Test
    public void initTest001() throws DisabledException {
        CommandReturn cr = null;
        cr = cliEnvironment.processInput("ls");
        System.out.print(cr.getReturn());
        cr = cliEnvironment.processInput("test -e hdfs://HOME90/user/dstreev/datasets/external/cc_trans_part/section=10");
        System.out.print(cr.getReturn());
        for (int i = 0; i < 10; i++) {
            cr = cliEnvironment.processInput("lsp -R -F \"([0-9]+_[0-9]+)|([0-9]+_[0-9]+_copy_[0-9]+)\" -i -Fe file -f parent,file /user/dstreev/bad_orc_files_test ");
            System.out.print(cr.getReturn());
            cr = cliEnvironment.processInput("lsp -f permissions_long,path /tmp/tpcds-generate/250/customer");
            System.out.print("** " + cr.getPath() + " " + cr.getReturn());
        }
    }
}
