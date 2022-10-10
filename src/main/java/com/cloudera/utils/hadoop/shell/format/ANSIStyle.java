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

package com.cloudera.utils.hadoop.shell.format;

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
