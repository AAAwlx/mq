package clients

import "net"

//消息分区信息
type PartKey struct{
	name string `json:"name"`
}

//Broker信息
type BrokerInfo struct {
	Name 		string `json:"name"`
	Host_port 	string `json:"hsotport"`
}
//函数获取本机所有网络接口的 MAC 地址
func GetIpport() string{
	interfaces, err := net.Interfaces()
	ipport := ""
	if err != nil {
		panic("Poor soul, here is what you got: " + err.Error())
	}
	for _, inter := range interfaces {
		mac := inter.HardwareAddr //获取本机MAC地址
		ipport += mac.String()
	}
	return ipport
}