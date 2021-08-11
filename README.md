# MYES.Sync
Mysql to elasticsearch sync tool.  Mysql批量导入Elasticsearch工具。

# Config File (myes.yaml) / 配置参数

``` yaml
MySqlConnectionString: server=localhost;port=3306;uid=test;pwd=test;charset=utf8;sslmode=none;
SyncDatabases: []
IgnoreDatabases: []
ElasticSearchUris: ["http://localhost:9200"]
IndexPrefix: index
NumberOfReplicas: 1
NumberOfShards: 3
```

