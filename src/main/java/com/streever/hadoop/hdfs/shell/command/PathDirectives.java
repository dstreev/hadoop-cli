package com.streever.hadoop.hdfs.shell.command;

public class PathDirectives {
    private Direction directionContext = null;

    private int directives = 0;
    private boolean directivesBefore = true;
    private boolean directivesOptional = false;

    public Direction getDirectionContext() {
        return directionContext;
    }

    public int getDirectives() {
        return directives;
    }

    public boolean isDirectivesBefore() {
        return directivesBefore;
    }

    public boolean isDirectivesOptional() {
        return directivesOptional;
    }

    public PathDirectives(Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        this.directionContext = directionContext;
        this.directives = directives;
        this.directivesBefore = directivesBefore;
        this.directivesOptional = directivesOptional;
    }
}
