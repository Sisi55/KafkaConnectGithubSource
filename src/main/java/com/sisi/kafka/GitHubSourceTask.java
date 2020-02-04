package com.sisi.kafka;

import com.sisi.kafka.model.Issue;
//import org.apache.http.client.utils.DateUtils;
import com.sisi.kafka.model.PullRequest;
import com.sisi.kafka.model.User;
import com.sisi.kafka.utils.DateUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.sisi.kafka.GitHubSchemas.*;

public class GitHubSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory
            .getLogger(GitHubSourceTask.class);

    public GitHubSourceConnectorConfig config;

    protected Instant nextQuerySince;
    protected Integer lastIssueNumber;
    protected Integer nextPageToVisit = 1;
    protected Instant lastUpdatedAt;

    GitHubAPIHttpClient gitHubHttpAPIClient;

    @Override
    public String version(){
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String,String> map){
        // required to start task
        // this could be open connection to database
        config = new GitHubSourceConnectorConfig(map);
        initializeLastVariables();
        gitHubHttpAPIClient = new GitHubAPIHttpClient(config);
    }

    private void initializeLastVariables(){
        Map<String,Object> lastSourceOffset = null;
        // context 는 SourceTask 의 속성
        lastSourceOffset = context.offsetStorageReader()
                .offset(sourcePartition());

        if(lastSourceOffset == null){
            // initialize to 7 days ago
            nextQuerySince = config.getSince();
            lastIssueNumber = -1;

        } else{
            Object updatedAt = lastSourceOffset.get(UPDATED_AT_FIELD);
            Object issueNumber = lastSourceOffset.get(NUMBER_FIELD);
            Object nextPage = lastSourceOffset.get(NEXT_PAGE_FIELD);

            if(updatedAt != null && (updatedAt instanceof String)){
                nextQuerySince = Instant.parse((String) updatedAt);
            }
            if(issueNumber != null && (issueNumber instanceof String)){
                lastIssueNumber = Integer.valueOf((String) issueNumber);
            }
            if(nextPage != null && (nextPage instanceof String)){
                nextPageToVisit = Integer.valueOf((String) nextPage);
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException{

        gitHubHttpAPIClient.sleepIfNeed();

        // fetch data
        final ArrayList<SourceRecord> records = new ArrayList<>();
        JSONArray issues = gitHubHttpAPIClient.getNextIssues(
                nextPageToVisit, nextQuerySince
        );

        int i=0; // count results
        for(Object obj : issues){
            Issue issue = Issue.fromJson((JSONObject) obj);
            // array 에서 element 전달
            SourceRecord sourceRecord = generateSourceRecord(issue);
            records.add(sourceRecord);
            i += 1;
            lastUpdatedAt = issue.getUpdatedAt();
        }

        if(i > 0)
            log.info(String.format("Fetched %s record(s)", i));

        if(i == 100){
            // reached full batch
            // need to get next one
            nextPageToVisit += 1;
        }else{
            nextQuerySince = lastUpdatedAt.plusSeconds(1);
            nextPageToVisit = 1;
            gitHubHttpAPIClient.sleep();
        }
        return records;
    }

    private SourceRecord generateSourceRecord(Issue issue){

        return new SourceRecord(
                sourcePartition(),
                sourceOffset(issue.getUpdatedAt()),
                config.getTopic(),
                null,
                KEY_SCHEMA,
                buildRecordKey(issue),
                VALUE_SCHEMA,
                buildRecordValue(issue),
                issue.getUpdatedAt().toEpochMilli()
        );
    }

    @Override
    public void stop(){

    }

    private Map<String,String> sourcePartition(){
        Map<String,String> map = new HashMap<>();
        map.put(OWNER_FIELD, config.getOwnerConfig());
        map.put(REPOSITORY_FIELD, config.getRepoConfig());
        return map;
    }

    private Map<String,String> sourceOffset(Instant updatedAt){
        Map<String,String> map = new HashMap<>();
        map.put(UPDATED_AT_FIELD, DateUtils.MaxInstant(
                updatedAt, nextQuerySince
        ).toString());

        map.put(NEXT_PAGE_FIELD, nextPageToVisit.toString());
        return map;
    }

    private Struct buildRecordKey(Issue issue){
        // key schema
        Struct key = new Struct(KEY_SCHEMA)
                .put(OWNER_FIELD, config.getOwnerConfig())
                .put(REPOSITORY_FIELD, config.getRepoConfig())
                .put(NUMBER_FIELD, issue.getNumber());

        return key;
    }

    public Struct buildRecordValue(Issue issue){
        // Issue top level fields
        Struct valueStruct = new Struct(VALUE_SCHEMA)
                .put(URL_FIELD, issue.getUrl())
                .put(TITLE_FIELD, issue.getTitle())
                .put(CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
                .put(UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
                .put(NUMBER_FIELD, issue.getNumber())
                .put(STATE_FIELD, issue.getState());

        // User is mandatory
        User user = issue.getUser();
        Struct userStruct = new Struct(USER_SCHEMA)
                .put(USER_URL_FIELD, user.getUrl())
                .put(USER_ID_FIELD, user.getId())
                .put(USER_LOGIN_FIELD, user.getLogin());
        valueStruct.put(USER_FIELD, userStruct);

        // Pull request is optional
        PullRequest pullRequest = issue.getPullRequest();
        if (pullRequest != null) {
            Struct prStruct = new Struct(PR_SCHEMA)
                    .put(PR_URL_FIELD, pullRequest.getUrl())
                    .put(PR_HTML_URL_FIELD, pullRequest.getHtmlUrl());
            valueStruct.put(PR_FIELD, prStruct);
        }

        return valueStruct;

    }
}
