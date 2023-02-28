package base

type NetworkAddr struct {
	Ip   string `json:"ip"`
	Port int16  `json:"port"`
}
type ExtraInfo struct {
	BeNetworkMap map[int64]NetworkAddr `json:"be_network_map"`
	Token        string                `json:"token"`
}
