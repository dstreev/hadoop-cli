package com.dstreev.hadoop.util;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by dstreev on 2016-03-21.
 */
public class NamenodeParserTest {

    @Test
    public void getTopUserOpsString() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            List<String> userOpsList = njp.getTopUserOpRecords();
            for (String userOps : userOpsList) {
                System.out.println(userOps);
            }
//            assertTrue(njp.getTopUserOpRecords() != null);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getFSState() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            String fsState = njp.getFSState();
            System.out.println(fsState);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getNamenodeInfo() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            String nnInfo = njp.getNamenodeInfo();
            System.out.println(nnInfo);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getTopUserOpsStringStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            List<String> userOpsList = njp.getTopUserOpRecords();
            for (String userOps : userOpsList) {
                System.out.println(userOps);
            }
//            assertTrue(njp.getTopUserOpRecords() != null);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getFSStateStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            String fsState = njp.getFSState();
            System.out.println(fsState);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getNamenodeInfoStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            String nnInfo = njp.getNamenodeInfo();
            System.out.println(nnInfo);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getTopUserOpsStringStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            List<String> userOpsList = njp.getTopUserOpRecords();
            for (String userOps : userOpsList) {
                System.out.println(userOps);
            }
//            assertTrue(njp.getTopUserOpRecords() != null);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getFSStateStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            String fsState = njp.getFSState();
            System.out.println(fsState);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getNamenodeInfoStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            String nnInfo = njp.getNamenodeInfo();
            System.out.println(nnInfo);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

}
