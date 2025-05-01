# AMQP Agent Go

一个基于 Go 语言开发的 AMQP 代理服务，用于接收 TCP 连接并将消息转发到 RabbitMQ。

## 功能特点

- 监听 TCP 端口（默认 8080）
- 接收 JSON 格式的消息
- 支持动态配置 RabbitMQ 连接参数
- 自动声明交换机
- 支持消息时间戳
- 支持配置文件
- 支持消息重试机制
- 支持队列大小限制

## 配置

配置文件 `config.yaml` 包含以下设置：

```yaml
server:
  port: 8080
  host: "0.0.0.0"
queue:
  max_size: "1MB"  # 支持 1KB, 1MB, 1GB
```

- `port`: 服务器监听的端口号
- `host`: 服务器监听的地址（0.0.0.0 表示监听所有网络接口）
- `max_size`: 重试队列的最大大小，支持 KB、MB、GB 单位

## 消息格式

```json
{
    "url": "amqp://user:password@host:port/vhost",
    "exchange": "exchange_name",
    "exchange_type": "direct",
    "routing_key": "routing_key",
    "m": "message_content",
    "timestamp": 1234567890
}
```

## 构建

```bash
go build
```

## 运行

```bash
./amqp-agent-go
```

## 使用示例

使用 netcat 发送测试消息：

```bash
echo '{
    "url": "amqp://guest:guest@localhost:5672/",
    "exchange": "test_exchange",
    "exchange_type": "direct",
    "routing_key": "test_key",
    "m": "Hello RabbitMQ!",
    "timestamp": 1234567890
}' | nc localhost 8080
```

## 重试机制

当消息发送失败时（例如 RabbitMQ 连接断开），消息会被存储在本地队列中。系统会自动尝试重新发送这些消息：

- 重试间隔为 5 秒
- 如果重试失败，消息会重新加入队列
- 队列大小有上限，超过限制时会移除最旧的消息
- 单个消息大小不能超过队列最大限制