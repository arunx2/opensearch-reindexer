package main

import "testing"

func Test_getIndexSettingsRequest(t *testing.T) {
	type args struct {
		templateLocation string
		primaries        string
		shardSize        int
	}
	tests := []struct {
		name                     string
		args                     args
		wantIndexSettingsRequest string
	}{
		{"TestWithoutTemplate", args{"", "1", 4}, `{
		  "settings": {
			"refresh_interval": "-1",
			"number_of_replicas": 0,
			"number_of_shards":4
		  }
		}`},
		{"TestWithTemplate", args{"dummyLocation", "5", 4}, `{
		  "settings": {
			"refresh_interval": "-1",
			"number_of_replicas": 0
		  }
		}`},
		{
			"TestWithNoTemplateAndTargetShard", args{"", "5", -1}, `{
		  "settings": {
			"refresh_interval": "-1",
			"number_of_replicas": 0,
			"number_of_shards":5
		  }
		}`,
		},
		{
			"TestWithNoTemplateAndShard", args{"", "5", 0}, `{
		  "settings": {
			"refresh_interval": "-1",
			"number_of_replicas": 0,
			"number_of_shards":5
		  }
		}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotIndexSettingsRequest := getIndexSettingsRequest(tt.args.templateLocation, tt.args.primaries, tt.args.shardSize); gotIndexSettingsRequest != tt.wantIndexSettingsRequest {
				t.Errorf("getIndexSettingsRequest() = %v, want %v", gotIndexSettingsRequest, tt.wantIndexSettingsRequest)
			}
		})
	}
}
