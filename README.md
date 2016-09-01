如何使用

	n := service.NewNotifyService()
	k := kt.NewKafkaTimer(conf.Conf.Kafkatimer, n)
