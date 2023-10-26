package base

import "fmt"

type Backend struct {
	Id       int64
	Host     string
	BePort   uint16
	HttpPort uint16
	BrpcPort uint16
}

// Backend Stringer
func (b *Backend) String() string {
	return fmt.Sprintf("Backend: {Id: %d, Host: %s, BePort: %d, HttpPort: %d, BrpcPort: %d}", b.Id, b.Host, b.BePort, b.HttpPort, b.BrpcPort)
}

func (b *Backend) GetHttpPortStr() string {
	return fmt.Sprintf("%d", b.HttpPort)
}
