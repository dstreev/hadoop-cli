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

import com.cloudera.utils.hadoop.hdfs.shell.command.Constants;
import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/*
Used to track the current state of available and accessed namespaces during the session.

 */
@Component
@Slf4j
@Getter
@Setter
public class FileSystemOrganizer {

    private Map<String, FileSystemState> namespaces = new TreeMap<String, FileSystemState>();
    private FileSystemState defaultFileSystemState = null;
    private FileSystemState defaultOzoneFileSystemState = null;
    private FileSystemState currentFileSystemState = null;
//    private FileSystem distributedFileSystem = null;
//    private FileSystem localFileSystem = null;

    private Configuration config = null;
    private FileSystem distributedFileSystem = null;
    private FileSystem localFileSystem = null;

    public Boolean isDefaultFileSystemState(FileSystemState fss) {
        Boolean rtn = Boolean.FALSE;
        if (fss.equals(defaultFileSystemState)
            // can't support completion on OZONE yet WHEN it's not the fs.defaultFS.
//                || fss.equals(defaultOzoneFileSystemState)
        ) {
            rtn = Boolean.TRUE;
        }
        return rtn;
    }


    public FileSystemState getFileSystemState(String uri) {
        // Extract the namespace from the uri

        // Check for it in the existing namespaces.
        // If Available, get it and return

        // if not:  create and set
        //  NEED to see if command was successful before adding?

        return namespaces.get(uri.toUpperCase());
    }

    // Called from Environment Setup Bean.
    public void init(Configuration config) {
        setConfig(config);
        try {
            distributedFileSystem = FileSystem.get(config);
            localFileSystem = FileSystem.getLocal(config);
            String defaultFS = config.get("fs.defaultFS");
            String configuredNameservices = config.get("dfs.nameservices");
            if (configuredNameservices != null) {
                String[] nameservices = configuredNameservices.split(",");
                for (String nameservice : nameservices) {
                    if (defaultFS.contains(nameservice)) {
                        // Setup Default State
                        FileSystemState fss = new FileSystemState();
                        fss.setFileSystem(distributedFileSystem);
                        fss.setNamespace(nameservice);
                        fss.setProtocol("hdfs://");
                        fss.setWorkingDirectory(new Path("/"));
                        namespaces.put(nameservice.toUpperCase(), fss);
                        setDefaultFileSystemState(fss);
                        currentFileSystemState = fss;
                    } else {
                        FileSystemState fss = new FileSystemState();
                        fss.setFileSystem(distributedFileSystem);
                        fss.setNamespace(nameservice);
                        fss.setProtocol("hdfs://");
                        fss.setWorkingDirectory(new Path("/"));
                        namespaces.put(nameservice.toUpperCase(), fss);
                    }
                }
            } else {
                // Setup Default State
                FileSystemState fss = new FileSystemState();
                fss.setFileSystem(distributedFileSystem);
                fss.setNamespace(defaultFS.replace("hdfs://", ""));
                fss.setProtocol("hdfs://");
                fss.setWorkingDirectory(new Path("/"));
                namespaces.put(defaultFS.toUpperCase(), fss);
                setDefaultFileSystemState(fss);
                currentFileSystemState = fss;
            }
            // look for Ozone Services
            String oServices = config.get("ozone.om.service.ids");
            if (oServices != null) {
                String[] ozoneServices = oServices.split(",");
                for (String ozoneService : ozoneServices) {
                    FileSystemState fss = new FileSystemState();
                    fss.setFileSystem(distributedFileSystem);
                    fss.setNamespace(ozoneService);
                    fss.setProtocol("ofs://");
                    fss.setWorkingDirectory(new Path("/"));
//                setDefaultOzoneFileSystemState(fss);
                    namespaces.put(ozoneService.toUpperCase(), fss);
                }
            }
            String oService = config.get("ozone.service.id");
            if (oService != null) {
                FileSystemState fss = new FileSystemState();
                fss.setFileSystem(distributedFileSystem);
                fss.setNamespace(oService);
                fss.setProtocol("ofs://");
                fss.setWorkingDirectory(new Path("/"));
                setDefaultOzoneFileSystemState(fss);
                namespaces.put(oService.toUpperCase(), fss);
            }

            // Build the Local FileSystemState
            FileSystemState lfss = new FileSystemState();
            lfss.setFileSystem(localFileSystem);
//            lfss.setNamespace("LOCAL");
            lfss.setProtocol("file:");
            lfss.setWorkingDirectory(localFileSystem.getWorkingDirectory());
            namespaces.put(Constants.LOCAL_FS, lfss);
        } catch (IOException ioe) {
            log.error("Error initializing FileSystemOrganizer: {}", ioe.getMessage());
        }

    }

    public boolean isCurrentDefault() {
        if (getCurrentFileSystemState().equals(getDefaultFileSystemState())) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public boolean isCurrentLocal() {
        if (getCurrentFileSystemState().equals(getFileSystemState(Constants.LOCAL_FS))) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public String getPrompt() {
//        int width = jline.TerminalFactory.get().getWidth();

        StringBuilder sb = new StringBuilder();
//        Set<String> lclNss = namespaces.keySet();
//        for (String namespace : lclNss) {
//            FileSystemState lclFss = namespaces.get(namespace);
//            if (lclFss.equals(defaultFileSystemState)) {
//                sb.append(ANSIStyle.style(namespace, ANSIStyle.FG_BLUE, ANSIStyle.BLINK, ANSIStyle.UNDERSCORE)).append("\t:");
//            } else if (namespace.equalsIgnoreCase(Constants.LOCAL_FS)) {
//                sb.append(ANSIStyle.style(namespace, ANSIStyle.FG_YELLOW)).append("\t:");
//            } else {
//                sb.append(ANSIStyle.style(namespace, ANSIStyle.FG_RED)).append("\t:");
//            }
//            sb.append(lclFss.toDisplay());
//            sb.append("\n");
//        }
        sb.append(ANSIStyle.style(this.getCurrentFileSystemState().getWorkingDirectory().toString(), ANSIStyle.FG_MAGENTA));
        sb.append(ANSIStyle.style(" on ", ANSIStyle.FG_GREEN));
        sb.append(ANSIStyle.style(this.getCurrentFileSystemState().getURI(), ANSIStyle.FG_YELLOW));
        sb.append("\n");
        sb.append(ANSIStyle.RIGHT_ARROW);
//        sb.append(ANSIStyle.style(" $: ", ANSIStyle.FG_RED));
        return sb.toString();
    }
}
