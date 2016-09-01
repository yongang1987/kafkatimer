package kafkatimer

import (
	"fmt"
	kc "common/kafkatimer/consumer"
	"sync"
)

type Config struct {
	FirstTopicName    string
	Delays            []int
	DelayTopicPrefix  string
	FailedTopicName   string
	BrokerList        []string
	ZkPath            string
	ZKList            []string
	ProcessingTimeout int
	CommitInterval    int
	Log               string
	Pid               string
}

type KafkaTimer struct {
	c          Config
	wg         sync.WaitGroup
	workerList []*worker
}

func NewKafkaTimer(c Config, execer Execer) *KafkaTimer {
	var (
		from string
		to   string
	)
	kt := &KafkaTimer{
		c:  c,
		wg: sync.WaitGroup{},
	}

	delayQueueCount := len(c.Delays)
	for index, delay := range c.Delays {
		if index == 0 {
			from = c.FirstTopicName
		} else {
			from = fmt.Sprintf("%s%d", c.DelayTopicPrefix, index)
		}

		if index == delayQueueCount-1 {
			to = c.FailedTopicName
		} else {
			to = fmt.Sprintf("%s%d", c.DelayTopicPrefix, index+1)
		}

		w := &worker{
			name:       fmt.Sprintf("worker_%d", index),
			delay:      delay,
			from:       from,
			to:         to,
			brokerList: c.BrokerList,
			execer:     execer,
			firstTopic: c.FirstTopicName,
			config: kc.Config{
				GroupName:         from,
				Topics:            []string{from},
				ZkPath:            c.ZkPath,
				ZkList:            c.ZKList,
				ProcessingTimeout: c.ProcessingTimeout,
				CommitInterval:    c.CommitInterval,
			},
		}
		kt.workerList = append(kt.workerList, w)
		kt.wg.Add(1)
		go func() {
			w.loop()
			kt.wg.Done()
		}()
	}
	return kt
}

func (this *KafkaTimer) Close() {
	for _, w := range this.workerList {
		w.Close()
	}
	this.wg.Wait()
}
