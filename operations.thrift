// 定义命名空间为 go，并指定语言为 go
namespace go api

// 定义 PushRequest 结构体，表示推送请求的数据结构
struct PushRequest {
    1: i64 producer   // 生产者 ID，类型为 64 位整数
    2: string topic    // 主题，类型为字符串
    3: string key      // 键，类型为字符串
    4: string message  // 消息，类型为字符串
}

// 定义 PushResponse 结构体，表示推送响应的数据结构
struct PushResponse {
    1: bool ret  // 返回值，表示操作结果，类型为布尔值
}

// 定义 PullRequest 结构体，表示拉取请求的数据结构
struct PullRequest {
    1: i64 consumer  // 消费者 ID，类型为 64 位整数
    2: string topic   // 主题，类型为字符串
    3: string key     // 键，类型为字符串
}

// 定义 PullResponse 结构体，表示拉取响应的数据结构
struct PullResponse {
    1: string message  // 消息，类型为字符串
}

// 定义 Operations 服务，提供推送和拉取方法
service Operations {
    // 定义 push 方法，接受一个 PushRequest 请求，并返回一个 PushResponse 响应
    PushResponse push(1: PushRequest req)
    
    // 定义 pull 方法，接受一个 PullRequest 请求，并返回一个 PullResponse 响应
    PullResponse pull(1: PullRequest req)
}
