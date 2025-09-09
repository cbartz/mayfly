package main

type Queue interface {
	Push([]byte) error
}
