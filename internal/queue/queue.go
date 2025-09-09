package queue

type Queue interface {
	Push([]byte) error
}
