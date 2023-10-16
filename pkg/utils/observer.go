package utils

type Observer[T any] interface {
	Update(T)
}

type Subject[T any] interface {
	Register(Observer[T])
	Unregister(Observer[T])
	Notify(T)
}
