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

package com.streever.hadoop.shell.command;

import java.io.ByteArrayOutputStream;

public class CommandReturn {
    public static int GOOD = 0;
    public static int BAD = -1;

    private int code = 0;
    private String details = null;
    private ByteArrayOutputStream bufferedOutputStream = new ByteArrayOutputStream();
//    private String commandBufferedOutput = null;

    public boolean isError() {
        if (code < 0)
            return true;
        else
            return false;
    }

    public boolean isMessage() {
        if (code > 0)
            return true;
        else
            return false;
    }

    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        if (details != null)
            sb.append(getDetails()).append("\t");
        //sb.append(getClass());
        return sb.toString();
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public ByteArrayOutputStream getBufferedOutputStream() {
        return bufferedOutputStream;
    }

    public void setBufferedOutputStream(ByteArrayOutputStream buffer) {
        this.bufferedOutputStream = buffer;
    }

    public CommandReturn(int code) {
        this.code = code;
    }

    public CommandReturn(int code, String details) {
        this.code = code;
        this.details = details;
    }

    public String getReturn() {
        String rtn = new String(this.getBufferedOutputStream().toByteArray());
//        StringBuilder sb = new StringBuilder();
//        if (!this.isError()) {
//            byte[] crOut = this.getBufferedOutputStream().toByteArray();
//            for (int x = 0; x < crOut.length; x++) {
//                System.out.print((char) crOut[x]);
//            }
//        } else {
//            System.out.println("ERROR (code) : " + this.getCode());
//            System.out.println("     Summary : " + this.getSummary());
//        }
        return rtn;
    }
}
