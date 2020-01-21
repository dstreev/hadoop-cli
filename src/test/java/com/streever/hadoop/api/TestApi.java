/*
 *  Hadoop CLI
 *
 *  (c) 2016-2019 David W. Streever. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with David W. Streever, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with David W. Streever or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) David W. Streever PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) David W. Streever DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) David W. Streever IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 *  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, David W. Streever IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 *
 */

package com.streever.hadoop.api;

import com.streever.hadoop.HadoopSession;
import com.streever.hadoop.shell.command.CommandReturn;
import org.junit.Before;
import org.junit.Test;

public class TestApi {

    private HadoopSession shell;
    private HadoopSession shell2;

    @Before
    public void before() {
        shell = HadoopSession.get("shell1");
        shell2 = HadoopSession.get( "shell2");
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
        cr = shell2.processInput("cd /warehouse");
        System.out.print(cr.getReturn());
        for (int i = 0; i < 10; i++) {
            cr = shell.processInput("ls");
            System.out.print(cr.getReturn());
            cr = shell2.processInput("ls");
            System.out.print(cr.getReturn());
        }

//        System.out.println(cr);
    }
}
