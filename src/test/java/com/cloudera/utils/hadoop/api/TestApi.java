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

import com.cloudera.utils.hadoop.cli.HadoopSession;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import org.junit.Before;
import org.junit.Test;

public class TestApi {

    private HadoopSession shell;
    private HadoopSession shell2;

    @Before
    public void before() {
        shell = new HadoopSession();
        shell2 = new HadoopSession();
        try {
            String[] api = {"-api"};
//            boolean result = Arrays.stream(alphabet).anyMatch("A"::equals);
            shell.start(api);
            shell2.start(api);
//            shell.processInput("connect");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void Test001_init() {
        CommandReturn cr = null;
        cr = shell.processInput("ls");
        System.out.print(cr.getReturn());
        cr = shell2.processInput("test -e hdfs://HOME90/user/dstreev/datasets/external/cc_trans_part/section=10");
        System.out.print(cr.getReturn());
        for (int i = 0; i < 10; i++) {
            cr = shell.processInput("lsp -R -F \"([0-9]+_[0-9]+)|([0-9]+_[0-9]+_copy_[0-9]+)\" -i -Fe file -f parent,file /user/dstreev/bad_orc_files_test ");
            System.out.print(cr.getReturn());
            cr = shell2.processInput("lsp -f permissions_long,path /tmp/tpcds-generate/250/customer");
            System.out.print("** " + cr.getPath() + " " + cr.getReturn());
        }

//        System.out.println(cr);
    }
}
