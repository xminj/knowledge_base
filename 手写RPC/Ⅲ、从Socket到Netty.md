
**引言：Socket能跑，但撑不住**
[[Ⅱ、重构整个工程]]，阶段Ⅱ使用Socket跑通了RPC，但它活不过生产
*  BIO模型：一个链接一个线程，1000个客户端就是1000个线程
* 同步阻塞：发一个请求等一个响应，吞吐上不去
* 没有心跳：链接断了不知道
* 没有重连：断了就死了

阶段Ⅲ把Socket换到Netty。
但换的过程中比我想象的难，**不是把Socket换成Netty那么简短，而是整个模型通信模型都变了**。同步变异步，一个链接变成链接服用。需要解决协议粘包

# 一、为什么换Netty？

**方案对比**

| 方案            | 优点             | 缺点               |
| ------------- | -------------- | ---------------- |
| 原生Socket（BIO） | 简单             | 一个链接一个线程，撑不住并发   |
| Java NIO      | 高性能            | API复杂，粘包要手写，容易出错 |
| Netty         | 高性能、生态成熟、API友好 | 学习曲线陡            |
| Mina          | 类似Netty        | 社区活跃低            |
**选Netty的理由**
* 事实标准：Dubbo、RocketMQ、gRPC-Java 都用它，值得学习
* 编解码框架成熟：`ByteToMessageDecoder`让粘包处理有章可循
* 内置心跳：`IdleStateHandler`直接支持
**核心变化**

| 维度    | 阶段Ⅱ（Socket BIO） | 阶段Ⅲ（Netty NIO） |
| ----- | --------------- | -------------- |
| 线程模型  | 一个链接一个线程        | EventLoop多路复用  |
| 同步/异步 | 同步阻塞            | 异步Future       |
| 链接复用  | 每次新建            | Channel复用      |
| 粘包处理  | 手动长度前缀          | 自定义协议+编解码器     |
**最关键的变化：从"一个请求一个线程"到"多个请求复用一条链接"**
这一个变化，就引出了本篇最重要的-requestId

# 二、设计决策1：自定义协议怎么设计？

Netty只负责"传字节"，但**字节流没有消息边界**。就像水管里的水，你不知道那一滴是那一次接的
必须自己定义协议，告诉对方："这20个字节是头，后面N个字节是内容"

**协议设计**
```text
+--------+--------+--------+--------+--------+----------------+--------+--------+
| 魔数   | 版本   | 序列化 | 消息类型| 状态   | 请求ID          | 长度   | body   |
| 4B    | 1B     | 1B     | 1B     | 1B     | 8B           | 4B    | N B    |
+--------+--------+--------+--------+--------+----------------+--------+--------+
```
**总头长20个字节**
**每个字段的决策**

| 字段             | 为什么需要      | 能否省      |
| -------------- | ---------- | -------- |
| 魔数`0xCAFEBABE` | 非法链接第一时间拒绝 | 不能       |
| 版本             | 协议升级兼容旧客户端 | 不能       |
| 序列化类型          | 每条消息独立选序列化 | 不能       |
| 消息类型           | 区分请求/响应/心跳 | 不能       |
| 状态             | 响应成功/失败    | 可省但保留更清晰 |
| requestId      | 异步匹配核心     | 绝不能省     |
| 长度             | 粘包拆包必须     | 不能       |
**为什么魔数放最前面**：如果一个非法链接(比如HTTP请求)请求进来，读到前4个字节不匹配就立刻拒绝，不用等整个包读完，节省资源
**为什么长度放最后**：编码时先写头，body序列化后才知道长度；解码时先解头，每一个字节都在回答"对方需要知道什么"

# 三、设计决策2：为什么必须又requestId

这是阶段Ⅱ和阶段Ⅲ最本质的区别

**阶段Ⅱ的场景**
```text
客户端：发请求 → 等响应 → 拿到结果 → 连接关闭
```
一个请求一个链接，**响应必然属于这个请求**，不需要ID

**阶段Ⅲ的场景**
```text
客户端：发请求 1 ┐
       发请求 2 ├→ 同一条连接 → 响应乱序回来
       发请求 3 ┘
```
三个请求公用一个TCP链接，响应回来是**怎么知道是那个请求**
就需要使用每次请求携带的requestId，响应在返回这个Id。

**实现机制**
```java
// 客户端维护一个映射表
Map<Long, CompletableFuture<RpcResponse>> pendingRequests;

// 发送时
long requestId = idGenerator.incrementAndGet();
CompletableFuture<RpcResponse> future = new CompletableFuture<>();
pendingRequests.put(requestId, future);
channel.writeAndFlush(message);  // message 里带 requestId

// 响应回来时
CompletableFuture<RpcResponse> future = pendingRequests.remove(msg.getRequestId());

future.complete(msg.getData());
```

**一张图理解**
```text
客户端                                服务端
  │                                    │
  │── 请求(id=1) ──────────────────────>│
  │── 请求(id=2) ──────────────────────>│
  │── 请求(id=3) ──────────────────────>│
  │                                    │ 处理...
  │<────────────────── 响应(id=2, 4) ───│
  │<────────────────── 响应(id=1, 2) ───│
  │<────────────────── 响应(id=3, 6) ───│
  │                                    │
  future(id=1).complete(2)             │
  future(id=2).complete(4)             │
  future(id=3).complete(6)             │
```

# 四、设计决策3：粘包拆包怎么解决？

TCP是字节流，会出现两种情况
* 粘包：两次请求的数据粘在一起，一次收到
* 拆包：一次请求的数据被拆成两次收到

解决方案：长度前缀
```text
解码逻辑：
1、收数据
2、如果不够20个字节（头），直接return，等下次
3、标记当前读指针（markReaderIndex）
4、读请求头，解析出length
5、如果剩余字节 < length，说明body没有收全。回退到标记位置（resetReaderIndex），等下次
6、body收全了，读出来，反序列化
```
**关键代码**
```java
@Override
protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    // 1. 头都没收全
    if (in.readableBytes() < HEADER_LENGTH) {
        return;   // 等下次
    }

    // 2. 标记读指针
    in.markReaderIndex();

    // 3. 读头
    int magic = in.readInt();
    // ... 读其他字段
    int length = in.readInt();

    // 4. body 没收全，回退！
    if (in.readableBytes() < length) {
        in.resetReaderIndex();   // 关键！
        return;
    }

    // 5. 读 body
    byte[] body = new byte[length];
    in.readBytes(body);
    // ...
}
```
**为什么不行 `markReaderIndex`+ `resetReaderIndex`**
* 如果头收了但body没收全，头已经*被读了*，指针无法回退
* 下次数据来了，会从错误的位置开始读
* 结果就是：**从第二次包开始全乱**

# 五、设计决策4：为什么使用`SimpleChannelInboundHandler`

Netty有两个常用的入站处理基类：

| 基类                           | 是否自动release | 适用场景               |
| ---------------------------- | ----------- | ------------------ |
| ChannelInboundHandlerAdapter | 否，手动        | 需要传递msg给下一个handler |
| SimpleChannelInboundHandler  | 是，自动        | 处理完了就结束            |
选`SimpleChannelInboundHandler`：因为处理器处理完msg就完事了，不需要往下传
**代价**：如果需要把msg传给下一个handler，需要`ReferenceCountUtile.retain(msg)`，否则下一个handler拿到的是一个已释放的对象

# 六、问题记录
**编解码器字字段顺序不一致**
**`markReaderIndex`忘了reset，第二次解码全错**
**心跳响应被当成业务响应**
**链接断开后pendingRequests内存泄漏**
* 现象：服务端重启几次，客户端内存不断上涨
* 原因：链接断开后，`pendingRequests` 里的Future永远不会被唤醒。调用方在future.get(timeout)上超时了,但Future对象还被