package admin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func TestGetBrokerRuntimeStats(t *testing.T) {
	nameSrvAddr := []string{"127.0.0.1:9876"}
	brokerAddr := "127.0.0.1:10911"

	t.Run("GetBrokerRuntimeStats", func(t *testing.T) {
		mqAdmin, err := NewAdmin(
			WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		)
		if err != nil {
			t.Error(err.Error())
			return
		}
		stats, err := mqAdmin.GetBrokerRuntimeStats(context.Background(), brokerAddr, 3*time.Second)
		if err != nil {
			t.Error(err.Error())
			return
		}
		t.Logf("GetBrokerRuntimeStats: %#v", stats)
		t.Logf("GetBrokerRuntimeStats: %s %s", stats["consumeQueueDiskRatio"], stats["commitLogDiskRatio"])
	})
}

func TestExamineBrokerClusterInfo(t *testing.T) {
	nameSrvAddr := []string{"127.0.0.1:9876"}
	t.Run("ExamineBrokerClusterInfo", func(t *testing.T) {
		mqAdmin, err := NewAdmin(
			WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		)
		if err != nil {
			t.Error(err.Error())
			return
		}

		resp, err := mqAdmin.ExamineBrokerClusterInfo(context.Background(), 3*time.Second)
		if err != nil {
			t.Error(err.Error())
			return
		}
		t.Logf("ExamineBrokerClusterInfo: %#v", resp)
	})
}

func TestExamineFetchAllTopicList(t *testing.T) {
	nameSrvAddr := []string{"127.0.0.1:9876"}
	t.Run("ExamineFetchAllTopicList", func(t *testing.T) {
		mqAdmin, err := NewAdmin(
			WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		)
		if err != nil {
			t.Error(err.Error())
			return
		}
		resp, err := mqAdmin.FetchAllTopicList(context.Background())
		if err != nil {
			t.Error(err.Error())
			return
		}
		t.Logf("ExamineFetchAllTopicList: %#v", resp)
	})
}

func TestExamineConsumeStats1(t *testing.T) {
	nameSrvAddr := []string{"127.0.0.1:9876"}
	brokerAddr := "192.168.23.185:10911"
	consumerGroup := "docs_createDocFlowGroup"
	topic := "signsAfterCreateProcessTopic"
	t.Run("ExamineConsumeStats", func(t *testing.T) {
		mqAdmin, err := NewAdmin(
			WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		)
		if err != nil {
			t.Error(err.Error())
			return
		}
		resp, err := mqAdmin.ExamineConsumeStats(context.Background(), brokerAddr, consumerGroup, topic, 3*time.Second)
		if err != nil {
			t.Error(err.Error())
			return
		}
		by, _ := json.Marshal(resp)
		t.Logf("ExamineConsumeStats: %s", string(by))
	})
}
