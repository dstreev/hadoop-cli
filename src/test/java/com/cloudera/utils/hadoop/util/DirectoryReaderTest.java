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

package com.cloudera.utils.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by streever on 2016-02-15.
 */

public class DirectoryReaderTest {
    private static String SEPARATOR = ",";
    private static String NEW_LINE = "\n";
    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    //@Test
    public void ConnectToNamenode() {
        Long start = null; //= System.currentTimeMillis();
        Configuration cfg = new Configuration();
        try {
            FileSystem fs = FileSystem.get(cfg);
            Path path = new Path("/user/streever/directory_listing.csv");
            if (fs.exists(path))
                fs.delete(path,true);
            FSDataOutputStream out = fs.create(path);
            URI nnURI = fs.getUri();
//              String uriStr = "hdfs://HOME";
            DFSClient client;
//            URI nnURI = new URI(uriStr);
//            client = new DFSClient(cfg);
            client = new DFSClient(nnURI, cfg);
//            String file = "/user/streever/littlelog.csv";
            String file = "/user/streever";
            //HdfsFileStatus fileStatus = client.getFileInfo(file);
//            ClientProtocol cp = client.getNamenode();
//            HdfsFileStatus status = client.getFileInfo(file);
            start = System.currentTimeMillis();
            PathData pd = new PathData(file, cfg);
            MathContext mc = new MathContext(4, RoundingMode.HALF_UP);
            FileStatus status = pd.stat;
            if (status.isDirectory()) {
//                DirectoryListing dl = cp.getListing(file, null, true);
                PathData[] paths = pd.getDirectoryContents();
                for (PathData item: paths) {
                    StringBuilder sb = new StringBuilder();
                    FileStatus itemStatus = item.stat;
                    if (itemStatus.isDirectory()) {
                        System.out.println(itemStatus.getPath() + " is a directory");
                    } else {
                        sb.append(item.toString()).append(SEPARATOR);
                        System.out.println(item);
                        LocatedBlocks blocks = client.getLocatedBlocks(item.toString(), 0, Long.MAX_VALUE);
                        sb.append(itemStatus.getLen()).append(SEPARATOR);
                        sb.append(itemStatus.getBlockSize()).append(SEPARATOR);
                        sb.append(itemStatus.getReplication()).append(SEPARATOR);
                        sb.append(df.format(new Date(itemStatus.getAccessTime()))).append(SEPARATOR);
                        Double blockRatio = (double)itemStatus.getLen() / itemStatus.getBlockSize();
                        BigDecimal ratioBD = new BigDecimal(blockRatio,mc);
                        sb.append(ratioBD.toString()).append(SEPARATOR);

                        System.out.println("File size: " + blocks.getFileLength());
                        for (LocatedBlock block : blocks.getLocatedBlocks()) {
                            DatanodeInfo[] datanodeInfo = block.getLocations();
                            System.out.println("\tBlock: " + block.getBlock().getBlockName());

                            for (DatanodeInfo dni : datanodeInfo) {
                                System.out.println(dni.getIpAddr() + " - " + dni.getHostName());
                                StringBuilder sb1 = new StringBuilder(sb);
                                sb1.append(dni.getIpAddr()).append(SEPARATOR);
                                sb1.append(dni.getHostName()).append(SEPARATOR);
                                sb1.append(block.getBlock().getBlockName()).append(NEW_LINE);
                                out.write(sb1.toString().getBytes());
                            }
                        }

                    }
                }
//                DirectoryListing dl = cp.getListing(file, null, true);
                System.out.println("Directory");
            } else {
                LocatedBlocks blocks = client.getLocatedBlocks(file, 0, Long.MAX_VALUE);

                System.out.println("File size: " + blocks.getFileLength());
                for (LocatedBlock block : blocks.getLocatedBlocks()) {
                    DatanodeInfo[] datanodeInfo = block.getLocations();
                    System.out.println("\tBlock: " + block.getBlock().getBlockName());
                    for (DatanodeInfo dni : datanodeInfo) {
                        System.out.println(dni.getName());
                    }
                }
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Long end = System.currentTimeMillis();
        System.out.println("Time Lapse: " + Long.toString(end - start));
    }
}
