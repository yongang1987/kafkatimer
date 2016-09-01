如何使用

	type NotifyService struct {}

	func (this *NotifyService) Exec(data string) (pData string, err error) {
		pData = data
		return
	}

	n := service.NewNotifyService()
	k := kt.NewKafkaTimer(conf.Conf.Kafkatimer, n)
