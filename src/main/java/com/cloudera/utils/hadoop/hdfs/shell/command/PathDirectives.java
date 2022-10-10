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
