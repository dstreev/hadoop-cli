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

package com.cloudera.utils.hadoop.hdfs.util;

import com.cloudera.utils.hadoop.hdfs.shell.command.HdfsConnect;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.util.Date;

@Getter
@Setter
public class FileSystemState {

    private boolean partOfConfig = Boolean.FALSE;
    private FileSystem fileSystem = null;
    private String namespace = null;
    private String protocol = null;
    private Path workingDirectory = null;
    private Date lastAccessed = null;

    public String getHomeDir(CliEnvironment cliEnvironment) {
        StringBuilder sb = new StringBuilder();
        String userName = cliEnvironment.getProperties().getProperty(HdfsConnect.CURRENT_USER_PROP, System.getProperty("user.name"));
        if (fileSystem instanceof DistributedFileSystem) {
            String DISTRIBUTED_USER_HOME_BASE = "/user";
            return DISTRIBUTED_USER_HOME_BASE + "/" + userName;
        } else {
            String homeDir = System.getProperty("user.home");
            return homeDir;
        }
    }

    public String getURI() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.protocol);
        if (this.namespace != null) {
            sb.append(this.namespace);
        }
        return sb.toString();
    }

    public void setWorkingDirectory(Path workingDirectory) {
        // Strip the URI from the incoming path, if exists
        this.workingDirectory = new Path(workingDirectory.toString().replace(getURI(), ""));
        // Keep the Filesystem in sync
        fileSystem.setWorkingDirectory(new Path(workingDirectory.toString().replace(getURI(), "")));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileSystemState that = (FileSystemState) o;

        if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null) return false;
        return protocol != null ? protocol.equals(that.protocol) : that.protocol == null;
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
        return result;
    }

    public String toShortDisplay() {
        StringBuilder sb = new StringBuilder();
        sb.append(protocol);
        sb.append(namespace);
        return sb.toString();
    }

    public String toDisplay() {
        StringBuilder sb = new StringBuilder();
        sb.append(getURI());
        if (getWorkingDirectory() != null) {
            sb.append(getWorkingDirectory());
        }
        return sb.toString();
    }

}
