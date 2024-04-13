/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.cli;

public enum HadoopCliCommandLineOptionsEnum {

    INIT("i","init","file","Initialize the environment with the specified file"),
    EXECUTE("e","execute","command","Execute Command"),
    TEMPLATE("t","template","template","Template to use for the command"),
    TEMPLATE_DELIMITER("td","template-delimiter","delimiter","Delimiter to apply to 'input' for template option (default=',')"),
    STDIN("stdin","stdin",null,"Run Stdin pipe and Exit"),
    SILENT("s","silent",null,"Suppress Banner"),
    API("api","api",null,"API Mode"),
    VERBOSE("v","verbose",null,"Verbose Mode"),
    DEBUG("d","debug",null,"Debug Commands"),
    ENV_FILE("ef","env-file","file","Environment File(java properties format) with a list of key=values"),
    HELP("h","help",null,"Help");

    HadoopCliCommandLineOptionsEnum(String shortName, String longName, String argumentName, String description) {
    }

}
