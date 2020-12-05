package com.streever.hadoop;

import com.streever.hadoop.shell.command.CommandReturn;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HadoopSessionTest extends TestCase {

    private HadoopSession shell = null;

    private List<String> testList_01 = new ArrayList<String>(
            Arrays.asList(
                    "ls",
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
            ));

    public void setUp() throws Exception {
        super.setUp();
        shell = HadoopSession.get("shell1");
//        shell.start(new String[]{"connect"});
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
        CommandReturn cr = null;
        for (String command : testList_01) {
            System.out.println("Command: " + command);
            cr = shell.processInput(command);
            printCommandReturn(cr);
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