package com.sisi.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GitHubSourceConnector extends SourceConnector {

    private static Logger log = LoggerFactory
            .getLogger(GitHubSourceConnector.class);
    private GitHubSourceConnectorConfig config;

    @Override
    public String version(){
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String,String> map){
        config = new GitHubSourceConnectorConfig(map);
    }

    @Override // TODO
    public Class<? extends Task> taskClass(){
        return GitHubSourceTask.class;
    }

    @Override
    public List<Map<String,String>> taskConfigs(int i){
        // define individual task configurations
        ArrayList<Map<String,String>> configs =
                new ArrayList<>(1);
        configs.add(config.originalsStrings());
        return configs;
    }

    @Override
    public void stop(){
        // do things = necessary to stop connector
    }

    @Override
    public ConfigDef config(){
        return GitHubSourceConnectorConfig.conf();
    }
}
