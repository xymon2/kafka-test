package handler

import (
	"time"
)

var ConfigSet = TopicPartitionConfigMap{
	"godot": {
		0: {
			ResetOffset: true,
			OffsetTime:  time.Date(2024, 8, 27, 15, 28, 0, 0, time.Local),
		},
		1: {
			ResetOffset: false,
		},
	},
}

type TopicPartitionConfigMap map[string]map[int32]PartitionConfig

type PartitionConfig struct {
	ResetOffset bool
	OffsetTime  time.Time
}

var MessageCounter = map[string]int{}
