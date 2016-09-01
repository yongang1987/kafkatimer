package consumer

import (
	log "code.google.com/p/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
	//"gopkg.in/Shopify/sarama.v1"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

type Config struct {
	GroupName         string
	Topics            []string
	ZkPath            string
	ZkList            []string
	ProcessingTimeout int
	CommitInterval    int
}

type Consumer struct {
	c  Config
	cg *consumergroup.ConsumerGroup
	wg sync.WaitGroup
}

func NewConsumer(c Config) (consumer *Consumer, err error) {
//	log.Info("consumer config:%v", c)
	cg, err := newCg(c)
	if err != nil {
		return
	}
	consumer = &Consumer{
		c:  c,
		cg: cg,
		wg: sync.WaitGroup{},
	}
	consumer.wg.Add(1)
	go func() {
		for {
			err, ok := <-consumer.cg.Errors()
			if !ok {
				break
			}
			log.Error("consumer error(%v)", err)
		}
		consumer.wg.Done()
	}()
	return
}

func newCg(c Config) (cg *consumergroup.ConsumerGroup, err error) {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = time.Duration(c.ProcessingTimeout) * time.Second
	config.Offsets.CommitInterval = time.Duration(c.CommitInterval) * time.Second
	config.Zookeeper.Chroot = c.ZkPath
	cg, err = consumergroup.JoinConsumerGroup(c.GroupName, c.Topics, c.ZkList, config)
	if err != nil {
		return
	}
	return
}

func (this *Consumer) Messages() <-chan *sarama.ConsumerMessage {
	return this.cg.Messages()
}

func (this *Consumer) CommitUpto(msg *sarama.ConsumerMessage) error {
	return this.cg.CommitUpto(msg)
}

func (this *Consumer) Close() (err error) {
	err = this.cg.Close()
	this.wg.Wait()
	return
}
