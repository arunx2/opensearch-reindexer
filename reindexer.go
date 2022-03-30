package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"flag"
)

var (
	aliasName             string
	oldIndex              string
	newIndex              string
	url                   string
	userName              string
	password              string
	indexTemplateLocation string
	requestsPerSecond     int
	targetShardCount      int
	indexTemplateName     string
	xtraAlias             string
	slice                 string
	verbose               bool
	help                  bool
	Es                    *opensearch.Client
)

func initialize() {
	flag.StringVar(&aliasName, "alias", "", "Alias name. If an alias points to multiple indices, use old-index flag along with alias flag. If no alias defined, it can be skipped")
	flag.StringVar(&oldIndex, "index", "", "Existing index name. use alias flag if a new index need to be attached to the alias")
	flag.StringVar(&newIndex, "new-index", "", "New index name. optional flag. default behavior is to add timestamp in 2006-01-02-15-04 format")
	flag.StringVar(&xtraAlias, "extra-alias", "", "Additional alias name to update with new index")
	flag.StringVar(&url, "url", "", "Elasticsearch/OpenSearch url")
	flag.StringVar(&userName, "user", "", "Basic Auth: user name")
	flag.StringVar(&password, "password", "", "Basic Auth: password")
	flag.StringVar(&indexTemplateName, "template-name", "", "Template name. Default will be the template file name")
	flag.StringVar(&indexTemplateLocation, "template-location", "", "Absolute file path to the index template mapping")
	flag.StringVar(&slice, "slice", "auto", "automatic slicing. Specify the number of slices to use. to turn off slicing send 'disable'")
	flag.IntVar(&targetShardCount, "target-shard-count", -1, "Target Shard count. Default is to retain the same shard count. This will be ignored if template-location is present")
	flag.IntVar(&requestsPerSecond, "requests-per-second", -1, "Throttle the request. Default is disabled")
	flag.BoolVar(&verbose, "v", false, "Enable it when you need verbose output")
	flag.BoolVar(&help, "help", false, "Prints the above message")

	flag.Parse()
	if help {
		flag.PrintDefaults()
		os.Exit(0)
	}
	if (aliasName == "" && oldIndex == "") || (url == "") {
		//source index/alias and url are mandatory inputs to continue
		flag.PrintDefaults()
		os.Exit(0)
	}
	cfg := opensearch.Config{
		Addresses: []string{
			url,
		},
	}
	if userName != "" && password != "" {
		cfg.Username = userName
		cfg.Password = password
	}
	Es, _ = opensearch.NewClient(cfg)
	if verbose {
		fmt.Println("OpenSearch client initialized")
	}
}

func main() {
	initialize()
	start := time.Now()
	//publish new mapping template
	publishIndexTemplates(indexTemplateLocation, indexTemplateName)

	oldIndex, err := getCurrentIndexName()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	newIndex := getNewIndexName(oldIndex)

	//reindex from current index to new index
	err = reindex(oldIndex, newIndex)
	if err == nil {
		//replace the new index to alias
		err := switchAlias(aliasName, oldIndex, newIndex)
		if err == nil {
			fmt.Printf("New index %s has been completed with reindex from %s.", newIndex, oldIndex)
			if aliasName != "" {
				//if no alias name defined, don't close the index
				_ = closeIndex(oldIndex)
				fmt.Printf("\n Alias %s pointing to %s", aliasName, newIndex)
			}
		} else {
			fmt.Println("Error happened while switching alias")
			fmt.Print(err)
		}
	} else {
		fmt.Print(err)
		os.Exit(-1)
	}
	fmt.Printf("\nTook: %s", time.Since(start))

}

func publishIndexTemplates(indexFilePath string, indexTemplateName string) (success bool) {
	if filepath.IsAbs(indexFilePath) {
		fmt.Printf("%s is not an absolute path. Please prove absolute path of template file", indexFilePath)
		return
	}
	bytes, _ := ioutil.ReadFile(indexFilePath)
	if len(bytes) > 0 {
		if indexTemplateLocation == "" {
			indexTemplateName = filepath.Base(indexFilePath)
		}
		request := opensearchapi.IndicesPutIndexTemplateRequest{
			Body:   strings.NewReader(string(bytes)),
			Name:   indexTemplateName,
			Pretty: true,
		}
		_, err := request.Do(context.Background(), Es)
		if err == nil {
			if verbose {
				fmt.Printf("Index template published with a name %s and a templated located in %s", indexTemplateName, indexFilePath)
			}
			return true
		}
	}
	return false
}

func closeIndex(indexName string) (err error) {
	closeIndexRequest := opensearchapi.IndicesCloseRequest{
		Index: []string{indexName},
	}
	_, err = closeIndexRequest.Do(context.Background(), Es)
	if verbose {
		fmt.Printf("index %s is now closed. \n", indexName)
	}
	return err
}

func switchAlias(aliasName string, currentIndexName string, newIndexName string) (err error) {
	if aliasName == "" {
		if verbose {
			fmt.Println("\nNo alias name defined.")
		}
		return
	}
	updateAliasQuery := `{
  			"actions": [
    		{
      			"add": {
        		"index": "` + newIndexName + `",
        		"alias": "` + aliasName + `"
      		}	
    		},{
      			"remove": {
         			"index": "` + currentIndexName + `",
        			"alias": "` + aliasName + `"
      			}
    		}`
	if xtraAlias != "" {
		updateAliasQuery = updateAliasQuery + `,{
      			"add": {
        		"index": "` + newIndexName + `",
        		"alias": "` + xtraAlias + `"
      		}	
    		},{
      			"remove": {
         			"index": "` + currentIndexName + `",
        			"alias": "` + xtraAlias + `"
      			}
    		}`
	}
	updateAliasQuery = updateAliasQuery + `]}`
	aliasRequest := opensearchapi.IndicesUpdateAliasesRequest{
		Body: strings.NewReader(updateAliasQuery),
	}

	_, err = aliasRequest.Do(context.Background(), Es)
	if verbose {
		fmt.Printf("alias %s switched from %s to %s", aliasName, currentIndexName, newIndexName)
	}
	return
}

func reindex(currentIndexName string, newIndexName string) (err error) {
	/*
	 * 1. Check the existing index and get the settings
	 * 2. Create index with no replicas and disable refresh interval
	 * 3. after the reindex completion, restore the setting
	 */
	var refreshInterval, primaries, replicas string
	refreshInterval, primaries, replicas, err = getIndexSettings(currentIndexName)
	if err != nil {
		return
	}
	//create new index
	err = createNewIndexWithReindexSettings(newIndexName, primaries)
	if err != nil {
		return
	}
	reindexQuery := `{
  		"source": {
   			 "index": "` + currentIndexName + `"
  		},
		"dest": {
    		"index": "` + newIndexName + `"
  		}
	}`
	waitForCompletion := true
	reindexRequest := opensearchapi.ReindexRequest{
		Body:              strings.NewReader(reindexQuery),
		WaitForCompletion: &waitForCompletion,
		Pretty:            false,
		RequestsPerSecond: &requestsPerSecond,
	}
	if !strings.EqualFold(slice, "disable") {
		_, err := strconv.Atoi(slice)
		if err != nil {
			if slice != "auto" {
				fmt.Printf("\n%s is not a valid for slice. falling back to auto")
			}
			slice = "auto"
		}
	}
	reindexRequest.Slices = slice
	var response *opensearchapi.Response
	response, err = reindexRequest.Do(context.Background(), Es)
	if response != nil && response.StatusCode != 200 {
		all, _ := ioutil.ReadAll(response.Body)

		err = errors.New(string(all))
	} else {
		//reset the index settings
		restoreIndexSettings(newIndexName, refreshInterval, replicas)
	}
	return
}

func getIndexSettings(currentIndexName string) (refreshInterval string, primaries string, replicas string, err error) {
	indexSettingsRequest := opensearchapi.IndicesGetSettingsRequest{
		Index: []string{currentIndexName},
	}
	settingsResponse, err := indexSettingsRequest.Do(context.Background(), Es)

	if settingsResponse == nil || settingsResponse.StatusCode != http.StatusOK {
		fmt.Println("Index doesn't exist")
		return
	} else {
		jsonstr, _ := ioutil.ReadAll(settingsResponse.Body)
		refreshInterval = gjson.Get(string(jsonstr), currentIndexName+".settings.index.refresh_interval").String()
		replicas = gjson.Get(string(jsonstr), currentIndexName+".settings.index.number_of_replicas").String()
		primaries = gjson.Get(string(jsonstr), currentIndexName+".settings.index.number_of_shards").String()
	}
	return
}

func restoreIndexSettings(newIndexName string, refreshInterval string, replicas string) {
	updateSettingRequest := opensearchapi.IndicesPutSettingsRequest{
		Index: []string{newIndexName},
		Body: strings.NewReader(`{
		  "settings": {
			"refresh_interval": "` + refreshInterval + `"
			, "number_of_replicas": ` + replicas + `
		  }
		}`),
	}
	updateSettingResponse, _ := updateSettingRequest.Do(context.Background(), Es)
	if updateSettingResponse == nil || updateSettingResponse.StatusCode != http.StatusOK {
		if verbose {
			fmt.Printf("update index settings to %s failed!", newIndex)
		}
		return
	}
	if verbose {
		fmt.Printf("\nUpdate %s settings with original values(replicas %s and refresh interval %s)!", newIndexName, replicas, refreshInterval)
	}
}

func createNewIndexWithReindexSettings(newIndexName string, primaries string) (err error) {
	indexSettingsRequest := getIndexSettingsRequest(indexTemplateLocation, primaries, targetShardCount)
	indexCreateRequest := opensearchapi.IndicesCreateRequest{
		Index: newIndexName,
		Body:  strings.NewReader(indexSettingsRequest),
	}
	indexCreationResponse, err := indexCreateRequest.Do(context.Background(), Es)
	if indexCreationResponse == nil || indexCreationResponse.StatusCode != http.StatusOK {
		fmt.Printf("new index %s creation failed!\n", newIndex)
		if indexCreationResponse != nil {
			res, _ := ioutil.ReadAll(indexCreationResponse.Body)
			fmt.Printf("response %s", string(res))
		}
		return errors.New("\nindex creation failed with " + err.Error())
	}
	if verbose {
		fmt.Printf("\nNew index %s created with no replicas and refresh interval disabled", newIndexName)
	}
	return
}

func getIndexSettingsRequest(templateLocation string, primaries string, shardSize int) (indexSettingsRequest string) {
	indexSettingsRequest = `{
		  "settings": {
			"refresh_interval": "-1",
			"number_of_replicas": 0`
	if templateLocation == "" {
		indexSettingsRequest = indexSettingsRequest + `,
			"number_of_shards":`
		if shardSize > 0 {
			indexSettingsRequest = indexSettingsRequest + strconv.Itoa(shardSize)
		} else {
			indexSettingsRequest = indexSettingsRequest + primaries
		}
	}
	indexSettingsRequest = indexSettingsRequest + `
		  }
		}`
	return
}

func getNewIndexName(currentIndexName string) (indexName string) {
	if newIndex != "" {
		return newIndex
	}

	lastIndex := strings.LastIndex(currentIndexName, "_")
	currentTime := time.Now()
	indexSuffix := currentTime.Format("2006-01-02-15-04")
	var indexPrefix string
	if lastIndex > 0 {
		//runes := []rune(currentIndexName)
		indexPrefix = currentIndexName[:lastIndex]
	} else {
		indexPrefix = currentIndexName
	}
	indexName = indexPrefix + "_" + indexSuffix
	if verbose {
		fmt.Printf("\nCalculated new index name would be %s", indexName)
	}
	return
}

func getCurrentIndexName() (indexName string, err error) {
	if oldIndex != "" {
		indexName = oldIndex
		return
	}
	if aliasName != "" {
		if verbose {
			fmt.Println("Finding existing index name from alias")
		}
		//get the index pointed by alias
		request := opensearchapi.CatAliasesRequest{
			Name: []string{aliasName},
		}
		aliasResponse, err := request.Do(context.Background(), Es)
		if err == nil {
			//continue
			if aliasResponse.StatusCode == http.StatusOK {
				responseBody, _ := ioutil.ReadAll(aliasResponse.Body)
				split := strings.Split(string(responseBody), " ")
				//0th element is alias name 1st element is index name
				if len(split) > 2 {
					indexName = split[1]
					if verbose {
						fmt.Println("Found index name from alias ", indexName)
					}
				} else {
					fmt.Printf("\n alias %s doesn't exist", aliasName)
					err = errors.New("alias doesn't exist")
				}
			}
		}
	}
	return
}
