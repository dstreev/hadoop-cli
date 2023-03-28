package com.cloudera.utils.hadoop.yarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.List;
import java.util.Map;

public interface Stats {
    void execute();

    void process(CommandLine commandLine);

    void init(CommandLine commandLine);
    void clearCache();

    Options getOptions();

    Map<String, String[]> getRecordFieldMap();

    Map<String, List<Map<String, Object>>> getRecords();

    List<Map<String, Object>> getRecordList(String recordType);
}
