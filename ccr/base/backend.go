package base

import "fmt"

type Backend struct {
	Id            int64
	Host          string
	HeartbeatPort int16
	BePort        int16
	HttpPort      int16
	BrpcPort      int16
}

// Backend Stringer
func (b *Backend) String() string {
	return fmt.Sprintf("Backend: {Id: %d, Host: %s, HeartbeatPort: %d, BePort: %d, HttpPort: %d, BrpcPort: %d}", b.Id, b.Host, b.HeartbeatPort, b.BePort, b.HttpPort, b.BrpcPort)
}

func (b *Backend) GetHttpPortStr() string {
	return fmt.Sprintf("%d", b.HttpPort)
}
