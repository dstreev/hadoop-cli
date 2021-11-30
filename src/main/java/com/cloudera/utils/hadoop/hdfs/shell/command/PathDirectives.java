package com.cloudera.utils.hadoop.hdfs.shell.command;

public class PathDirectives {
    private Direction direction = Direction.NONE;

    private int directives = 0; //default
    private boolean before = true;  //default
    private boolean optional = false; //default

    public Direction getDirection() {
        return direction;
    }

    public int getDirectives() {
        return directives;
    }

    public boolean isBefore() {
        return before;
    }

    public boolean isOptional() {
        return optional;
    }

    public PathDirectives(Direction direction, int directives, boolean before, boolean optional) {
        this.direction = direction;
        this.directives = directives;
        this.before = before;
        this.optional = optional;
    }

    public PathDirectives(Direction direction, int directives, boolean before) {
        this.direction = direction;
        this.directives = directives;
        this.before = before;
    }

    public PathDirectives(Direction direction, int directives) {
        this.direction = direction;
        this.directives = directives;
    }

    public PathDirectives(Direction direction) {
        this.direction = direction;
    }

    public PathDirectives() {
    }
}
