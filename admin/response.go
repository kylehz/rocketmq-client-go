/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package admin

import (
	"encoding/json"
	"regexp"
	"strings"
)

type RemotingSerializable struct {
}

func (r *RemotingSerializable) Encode(obj interface{}) ([]byte, error) {
	jsonStr := r.ToJson(obj, false)
	if jsonStr != "" {
		return []byte(jsonStr), nil
	}
	return nil, nil
}

func (r *RemotingSerializable) ToJson(obj interface{}, prettyFormat bool) string {
	if prettyFormat {
		jsonBytes, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	} else {
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	}
}

/*
转换Java非标Json

ExamineBrokerClusterInfo: {"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:"192.168.1.111:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}
ExamineConsumeStats: {"consumeTps":0.0,"offsetTable":{{"brokerName":"broker-a","queueId":7,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0},{"brokerName":"broker-a","queueId":6,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0},{"brokerName":"broker-a","queueId":3,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0},{"brokerName":"broker-a","queueId":2,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0},{"brokerName":"broker-a","queueId":5,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0},{"brokerName":"broker-a","queueId":4,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0},{"brokerName":"broker-a","queueId":1,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0},{"brokerName":"broker-a","queueId":0,"topic":"topic_test"}:{"brokerOffset":0,"consumerOffset":0,"lastTimestamp":0}}}
*/
var javaJsonRegexp1 = regexp.MustCompile(`[{,]{.*?}:`)
var javaJsonRegexp2 = regexp.MustCompile(`[{,]\d*?:`)
var replacer1 = strings.NewReplacer(`{{`, `{"{`, `,{`, `,"{`, `}:`, `}":`, `"`, `\"`)
var replacer2 = strings.NewReplacer(`{`, `{"`, `,`, `,"`, `:`, `":`)

func replaceJavaJsonToGo(str string) string {
	str = javaJsonRegexp1.ReplaceAllStringFunc(str, func(repl string) string {
		repl = replacer1.Replace(repl)
		return repl
	})
	str = javaJsonRegexp2.ReplaceAllStringFunc(str, func(repl string) string {
		repl = replacer2.Replace(repl)
		return repl
	})
	return str
}

func (r *RemotingSerializable) Decode(data []byte, classOfT interface{}) (interface{}, error) {
	jsonStr := string(data)
	jsonStr = replaceJavaJsonToGo(jsonStr)
	return r.FromJson(jsonStr, classOfT)
}

func (r *RemotingSerializable) FromJson(jsonStr string, classOfT interface{}) (interface{}, error) {
	err := json.Unmarshal([]byte(jsonStr), classOfT)
	if err != nil {
		return nil, err
	}
	return classOfT, nil
}

type TopicList struct {
	TopicList  []string
	BrokerAddr string
	RemotingSerializable
}

type SubscriptionGroupWrapper struct {
	SubscriptionGroupTable map[string]SubscriptionGroupConfig
	DataVersion            DataVersion
	RemotingSerializable
}

type DataVersion struct {
	Timestamp int64
	Counter   int32
}

type SubscriptionGroupConfig struct {
	GroupName                      string
	ConsumeEnable                  bool
	ConsumeFromMinEnable           bool
	ConsumeBroadcastEnable         bool
	RetryMaxTimes                  int
	RetryQueueNums                 int
	BrokerId                       int
	WhichBrokerWhenConsumeSlowly   int
	NotifyConsumerIdsChangedEnable bool
}

type GroupList struct {
	GroupList []string
	RemotingSerializable
}

type ConsumeStatsOffsetMeta struct {
	BrokerName string `json:"brokerName"`
	QueueId    int    `json:"queueId"`
	Topic      string `json:"topic"`
}
type ConsumeStats struct {
	ConsumeTps  float32 `json:"consumeTps"`
	OffsetTable map[string]struct {
		Meta           ConsumeStatsOffsetMeta `json:"meta"`
		BrokerOffset   int                    `json:"brokerOffset"`
		ConsumerOffset int                    `json:"consumerOffset"`
		LastTimestamp  int                    `json:"lastTimestamp"`
	} `json:"offsetTable"`
	RemotingSerializable
}

func (s *ConsumeStats) ComputeTotalDiff() int64 {
	var diffTotal int64
	for _, v := range s.OffsetTable {
		diffTotal += int64(v.BrokerOffset - v.ConsumerOffset)
	}
	return diffTotal
}

type ClusterInfo struct {
	BrokerAddrTable map[string]struct {
		BrokerAddrs map[string]string `json:"brokerAddrs"`
		BrokerName  string            `json:"brokerName"`
		Cluster     string            `json:"cluster"`
	} `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string `json:"clusterAddrTable"`
	RemotingSerializable
}

type BrokerData struct {
	Cluster     string           `json:"cluster"`
	BrokerName  string           `json:"brokerName"`
	BrokerAddrs map[int64]string `json:"brokerAddrs"`
}

func (b *BrokerData) SelectBrokerAddr() string {
	if addr, ok := b.BrokerAddrs[0]; ok {
		return addr
	}
	for _, addr := range b.BrokerAddrs {
		return addr
	}
	return ""
}

type TopicRouteData struct {
	OrderTopicConf string `json:"orderTopicConf"`
	QueueDatas     []struct {
		BrokerName     string `json:"brokerName"`
		ReadQueueNums  int    `json:"readQueueNums"`
		WriteQueueNums int    `json:"writeQueueNums"`
		Perm           int    `json:"perm"`
		TopicSysFlag   int    `json:"topicSysFlag"`
	} `json:"queueDatas"`
	BrokerDatas       []BrokerData        `json:"brokerDatas"`
	FilterServerTable map[string][]string `json:"filterServerTable"`
	RemotingSerializable
}

type BrokerRuntimeStatsBind struct {
	Table map[string]string `json:"table"`
	RemotingSerializable
}
