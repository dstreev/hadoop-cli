// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.format;

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
    static final char ESC = 27;
    
    private static final String START = "\u001B[";
    private static final String END = "m";
    
    
    public static String style(String input, int... styles){
        StringBuffer buf = new StringBuffer();
        for(int style : styles){
            buf.append(START);
            buf.append(style);
            buf.append(END);
        }
        buf.append(input);
        buf.append(START);
        buf.append(OFF);
        buf.append(END);
        return buf.toString();
    }
}
