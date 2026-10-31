package pubsub

type SimpleQueueType string

const (
	QueueTypeDurable   SimpleQueueType = "durable"
	QueueTypeTransient SimpleQueueType = "transient"
)

type AckType int

const (
	TypeAck AckType = iota
	TypeNackRequeue
	TypeNackDiscard
)