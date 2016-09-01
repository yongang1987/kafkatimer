package producer
import (
	log "code.google.com/p/log4go"
	"errors"
//	"gopkg.in/Shopify/sarama.v1"
	"github.com/Shopify/sarama"
)

var (
	NoBrokerList = errors.New("no broker list")
)

type Producer struct {
	p sarama.SyncProducer
}

func NewProducer(brokerList []string) (pro *Producer, err error) {
	if len(brokerList) == 0 {
		err = NoBrokerList
		return
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewHashPartitioner
	p, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Error("sarama.NewSyncProducer(%v, config) error(%v)", brokerList, err)
		return
	}
	pro = &Producer{
		p: p,
	}
	return
}

func (this *Producer) Send(topic string, key string, data string) (err error) {
	message := &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(data)}
	_, _, err = this.p.SendMessage(message)
	if err != nil {
		log.Error("producer.SendMessage(message) key: %s, data: %s error(%v)", key, data, err)
		return
	}
	return
}


func (this *Producer) Close() (err error) {
	err = this.p.Close()
	return
}
