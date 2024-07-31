// PushRequest 定义了推送请求的结构
struct PushRequest {
    1: i64 producer  // 生产者的 ID，使用 64 位整数
    2: string topic  // 主题名称
    3: string key    // 消息的键（可用于分片或标识）
    4: string message // 要推送的消息内容
}

// PushResponse 定义了推送响应的结构
struct PushResponse {
    1: bool ret       // 推送操作的结果，布尔值表示成功（true）或失败（false）
}

// PullRequest 定义了拉取请求的结构
struct PullRequest {
    1: i64 consumer   // 消费者的 ID，使用 64 位整数
    2: string topic   // 主题名称
    3: string key     // 消息的键（可用于分片或标识）
}

// PullResponse 定义了拉取响应的结构
struct PullResponse {
    1: string message // 拉取到的消息内容
}

// InfoRequest 定义了获取信息请求的结构
struct InfoRequest {
    1: string ip_port // 客户端的 IP 和端口
}

// InfoResponse 定义了获取信息响应的结构
struct InfoResponse {
    1: bool ret       // 获取信息操作的结果，布尔值表示成功（true）或失败（false）
}

// Server_Operations 定义了服务器操作的服务接口
service Server_Operations {
    PushResponse push(1: PushRequest req) // 推送消息的方法
    PullResponse pull(1: PullRequest req) // 拉取消息的方法
    InfoResponse info(1: InfoRequest req) // 获取信息的方法
}

// PubRequest 定义了发布请求的结构
struct PubRequest {
    1: string meg // 要发布的消息内容（注意这里有个小错误，应该是 "message"）
}

// PubResponse 定义了发布响应的结构
struct PubResponse {
    1: bool ret   // 发布操作的结果，布尔值表示成功（true）或失败（false）
}

// PingPongRequest 定义了 ping-pong 请求的结构
struct PingPongRequest {
    1: bool ping  // 布尔值，表示是否发送 ping 请求
}

// PingPongResponse 定义了 ping-pong 响应的结构
struct PingPongResponse {
    1: bool pong  // 布尔值，表示收到的 pong 响应
}

// Client_Operations 定义了客户端操作的服务接口
service Client_Operations {
    PubResponse pub(1: PubRequest req) // 发布消息的方法
    PingPongResponse pingpong(1: PingPongRequest req) // ping-pong 方法
}
