package transport

type DataTransport interface {
	StartProducer() error
	StopProducer() error
	ProducerEnabled() bool

	Write(databaseConnectionId string, table string, data []byte) error

	StartConsumer() error
	StopConsumer() error
	ConsumerEnabled() bool
}
