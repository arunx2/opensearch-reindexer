# Reindexer

Reindexer is a handy command line utility helps reindexing process in elasticsearch (upto V7.10.2) or OpenSearch. It follows best practices for speed up the indexing process. It disables the index refresh interval and replicas. It restores the same index settings after completion. It has ability to parallelize the reindexing process with slicing. It automatically closes the index and switch alias to new index.

Refer: OpenSearch reindex [documentation](https://opensearch.org/docs/latest/opensearch/reindex-data/) for slicing and throttling

Binaries can be downloaded [here](https://gitlab.aws.dev/arnlaksh/reindex-tool/-/tree/master/binaries) 
### Examples
**Simple reindexing**
```shell
$ reindexer -url https://<<search_endpoint>> \
  -index my_index 
```
```shell
$ reindexer -url https://<<search_endpoint>> \
  -index my_index \
  -user <<username>> \
  -password <<passpword>>
```
This creates a new index with timestamp appended to it. (eg.my_index-2021-07-28-12-30) 

**Reindex with target primary shard count**
```shell
$ reindexer -url https://<<search_endpoint>> \
  -index my_index \
  -target-shard-count 1
```
This creates a new single sharded index

**Reindex with index alias**
```shell
$ reindexer -url https://<<search_endpoint>> \
  -alias my_index \
```
This re-indexes the first index pointed by the alias with a same name appended with timestamp. It closes the previous index and switch the alias pointing to new index if successful.


**Usage of reindexer**
```
-alias string
    Alias name. If an alias points to multiple indices, use old-index flag along with alias flag. If no alias defined, it can be skipped
-url string
    Elasticsearch or OpenSearch url
-index string
    Existing index name. use alias flag if a new index need to be attached to the alias
-new-index string
    New index name. optional flag. default behavior is to add timestamp in 2006-01-02-15-04 format
-password string
    Basic Auth: password
-requests-per-second int
    Throttle the request. Default is disabled (default -1)
-template-location string
    Absolute file path to the index template mapping
-template-name string
    Template name. Default will be the template file name
-target-shard-count string
    Target Shard size. Default is -1 which means to retain the same shard count. This will be ignored if template-location is present
 -slice string
    automatic slicing. Specify the number of slices to use. to turn off slicing send disable (default "auto")
-user string
    Basic Auth: user name
-help
    Prints the above message

-v    Enable it when you need verbose output
```
