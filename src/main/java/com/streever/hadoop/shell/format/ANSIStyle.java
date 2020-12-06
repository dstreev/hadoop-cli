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

// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.streever.hadoop.shell.format;

public class ANSIStyle {
    public static final int OFF = 0;
    public static final int BOLD = 1;
    public static final int UNDERSCORE = 4;
    public static final int BLINK = 5;
    public static final int REVERSE = 7;
    public static final int CONCEALED = 8;
    public static final int FG_BLACK = 30;
    public static final int FG_RED = 31;
    public static final int FG_GREEN = 32;
    public static final int FG_YELLOW = 33;
    public static final int FG_BLUE = 34;
    public static final int FG_MAGENTA = 35;
    public static final int FG_CYAN = 36;
    public static final int FG_WHITE = 37;
    public static final String RIGHT_ARROW = "\u2794";
    static final char ESC = 27;
    
    private static final String START = "\u001B[";
    private static final String END = "m";
    
    public static class StyleWrapper {
        private String format;
        private Integer[] styles;

        public StyleWrapper(String format, Integer[] styles) {
            this.format = format;
            this.styles = styles;
        }

        public String getFormat() {
            return format;
        }

        public Integer[] getStyles() {
            return styles;
        }
    }

    public static String style(String input, StyleWrapper wrapper) {
        // TODO: Add FORMATTING with wrapper.getFormat();
        return style(input, wrapper.getStyles());
    }

    public static String style(String input, Integer... styles){
        StringBuffer buf = new StringBuffer();
        if (styles != null) {
            for (int style : styles) {
                buf.append(START);
                buf.append(style);
                buf.append(END);
            }
            buf.append(input);
            buf.append(START);
            buf.append(OFF);
            buf.append(END);
        } else {
            buf.append(input);
        }
        return buf.toString();
    }
}
