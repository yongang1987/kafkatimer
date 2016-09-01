package kafkatimer

import (
	log "code.google.com/p/log4go"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	kc "common/kafkatimer/consumer"
	kp "common/kafkatimer/producer"
	"sync"
	"time"
)

type Execer interface {
	Exec(data string) (pData string, err error)
}

type qMsg struct {
	Data string `json:"data"`
	Ts   int64  `json:"ts"` // us
}

type worker struct {
	config     kc.Config
	name       string
	from       string
	to         string
	delay      int
	consumer   *kc.Consumer
	producer   *kp.Producer
	brokerList []string
	execer     Execer
	firstTopic string
	closed     bool
}

func (this *worker) loop() {
	log.Info("[ %s ] start, from:%s, to:%s, delay:%d", this.name, this.from, this.to, this.delay)
	for {
		if this.closed {
			break
		}
		if err := this.connect(); err != nil {
			log.Error("this.connect() error(%v)", err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Info("[ %s ] connect ok, from:%s, to:%s, delay:%d", this.name, this.from, this.to, this.delay)
		this.handle()
//		this.close()
		log.Info("[ %s ] close, from:%s, to:%s, delay:%d", this.name, this.from, this.to, this.delay)
	}
	log.Info("[ %s ] stop, from:%s, to:%s, delay:%d", this.name, this.from, this.to, this.delay)
}

func (this *worker) handle() {
	var data string
	var ts int64
	var wg sync.WaitGroup
	for {
		m, ok := <- this.consumer.Messages()
		if !ok {
			break
		}
		ts = -1
		if this.from == this.firstTopic {
			data = string(m.Value)
		} else {
			qmsg := &qMsg{}
			if err := json.Unmarshal(m.Value, qmsg); err != nil {
				log.Error("json.Unmarshal(%s, struct) error(%v)", m.Value, err)
				continue
			}
			data = qmsg.Data
			ts = qmsg.Ts
		}
		if ts > 0 {
//			diff := ts - time.Now().UnixNano()
//			if diff > 0 {
//				log.Info("[ %s ] not reach time, sleep %d second", this.name, int(time.Duration(diff)/time.Second))
//				time.Sleep(time.Nanosecond * time.Duration(diff))
//			}


			/***
			防止sleep时无法退出
			 */
			for ; !this.closed && ts > time.Now().UnixNano();{
				time.Sleep(10 * time.Millisecond)
			}
			if this.closed {
				break
			}
		}
		wg.Add(1)
		go func() {
			this.asyncProcess(data)
			wg.Done()
		}()
		if err := this.consumer.CommitUpto(m); err != nil {
			log.Error("[ %s ] consumer.CommitUpto(%v) error(%v)", this.name, m, err)
			break
		}
	}
	wg.Wait()
}

func (this *worker) asyncProcess(data string) {
	pData, err := this.execer.Exec(data)
	if err == nil {
		log.Info("[ %s ], from:%s, to:%s, delay:%d, data:%s, pdata:%s, process success", this.name, this.from, this.to, this.delay, data, pData)
		return
	}
	qmsg := &qMsg{
		Data: pData,
		Ts:   time.Now().Add(time.Second * time.Duration(this.delay)).UnixNano(),
	}
	msgBytes, err := json.Marshal(qmsg)
	if err != nil {
		log.Error("json.Marshal(%s) error(%v)", qmsg, err)
		return
	}
	h := md5.New()
	key := hex.EncodeToString(h.Sum(msgBytes))
	if err := this.producer.Send(this.to, key, string(msgBytes)); err != nil {
		log.Error("[ %s ] send to %s failed error(%v)", this.name, this.to, err)
		return
	}
	log.Info("[ %s ], from:%s, to:%s, delay:%d, data:%s, pdata:%s, process failed, push to next", this.name, this.from, this.to, this.delay, data, pData)
}

func (this *worker) connect() (err error) {
	producer, err := kp.NewProducer(this.brokerList)
	if err != nil {
		log.Error("kc.NewProducer(%v) error(%v)", this.brokerList, err)
		return
	}
	this.producer = producer
	consumer, err := kc.NewConsumer(this.config)
	if err != nil {
		log.Error("kc.NewConsumer(%v) error(%v)", this.config, err)
		this.close()
		return
	}
	this.consumer = consumer
	return
}

func (this *worker) Close() {
	this.closed = true
	this.close()
}

func (this *worker) close() {
	if this.consumer != nil {
		if err := this.consumer.Close(); err != nil {
			log.Error("this.consumer.Close() error(%v)", err)
		}
		this.consumer = nil
	}
	if this.producer != nil {
		if err := this.producer.Close(); err != nil {
			log.Error("this.producer.Close() error(%v)", err)
		}
		this.producer = nil
	}
}
