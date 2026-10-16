
[[Ⅳ、SPI 可插拔 + 三种序列化实现]]跑通了序列化，但服务注册地址都写死在内存Map中。跨进程无法发现注册的服务地址
```java
// Consumer 里手动塞地址
SimpleServiceRegistry registry = new SimpleServiceRegistry();
registry.register(HelloService.class.getName(), "127.0.0.1:8080");
```
问题很明显：
* 跨进程不可用：Prodiver注册在它自己的进程里，Consumer根本看不到
* Provider挂了不知道：Consumer一致调用一个死地址
* 无法动态扩容：加一个Provide要手动修改Consumer代码

在本阶段把注册中心换成Zookeeper，让服务自动注册、自动发现、自动感知上下线。

# 一、为什么使用Zookeeper？
候选方案对比

| 方案        | 一致性     | 成熟度         | 运维成本 | 与RPC场景契合度     |
| --------- | ------- | ----------- | ---- | ------------- |
| Zookeeper | CP      | 高（Dubbo老搭档） | 中    | 临时节点天然契合      |
| Nacos     | AP/CP可选 | 高           | 低    | 功能更全          |
| Eureke    | AP      | 中           | 低    | 偏Spring Cloud |
| Etcd      | CP      | 高           | 中    | 偏k8s          |
选Zookeeper的理由
* 临时节点的设计哲学：服务进程一死，节点自动消失-不需要任何格外的健康检测逻辑
* Dubbo的默认机制：资料多，遇到问题好查
* Watcher机制能实时感知节点变化
* Curator封装解决了原生ZK API的痛点
代价：
* CP模型，ZK集群不可用时整个注册中心不可用
* 运维比Nacos稍复杂
* Watcher在大规模客户端场景可能有"惊群效应"

# 二、设计决策1：ZK节点结构怎么设计

```text
/minirpc                                               (持久节点，根目录)
	|_org.xminj.minirpc.api.HelloService               (持久节点，服务名)
		|_ 192.168.1.100                               (临时节点，provide 1)
		|_ 192.168.1.101                               (临时节点，provide 1)       
```
为什么这样设计

| 节点              | 类型  | 原因                    |
| --------------- | --- | --------------------- |
| /minirpc        | 持久  | 跟目录永远存在               |
| /minirpc/服务名    | 持久  | 即使所有Provide下线，目录也不该消失 |
| /minirpc/服务名/地址 | 临时  | Provide进程死，节点自动消失     |
如果Provide节点也持久会怎么样
* 进程挂了节点还在
* Consumer一直调用一个死地址
* 要人工清理节点

如果使用临时节点
* 进程死，会话断，ZK自动删除
* Consumner通过Watcher立刻感知

# 三、设计决策2：为什么使用Curator，不用原生ZK api

原生Zookeeper API有三个致命问题
* Watch 一次性失效
	```java
// 原生 API：注册一次，只触发一次
zk.getChildren(path, event -> {
    // 处理事件
    // 处理完必须重新注册！否则下次收不到
    zk.getChildren(path, this);  // 忘记这行 = 永远收不到通知
});
	```
* 不支持递归创建节点
```java
// 创建 /rpc/com.example.HelloService/127.0.0.1:8080
// 必须先创建 /rpc，再创建 /rpc/服务名，再创建地址节点
// 原生 API 每层都要 checkExists + create
```
* 异常处理繁琐
  NodeExistsException、ConnectionLossException、SessionExpiredException...没种异常要单独处理

Curator解决了这些问题
* CuratorCache：自动重新注册Watcher
* **`creatingParentsIfNeeded()`** 自动递归创建
* **`ExponentialBackoffRetry`**自动重试
* API简洁，Fluent风格


# 四、设计决策3：为什么需要本地缓存

如果没有缓存：每次discover()都查ZK。
```text
Consumer 调用 1000 次/秒
  → ZK 收到 1000 次查询/秒
  → ZK 压力大，延迟高，可能成为瓶颈
```

如果只能缓存不监听：Provide上下线后，Consumer不知道，一直用旧地址
所有：缓存+Watcher组合
* 正常情况下缓存，快，不查ZK
* zk变化时更新缓存，准，靠Watcher
实现：
```java
private final Map<String, List<String>> cache = new ConcurrentHashMap<>();

@Override
public List<String> discover(String serviceName) {
    // 1. 先查缓存
    List<String> cached = cache.get(serviceName);
    if (cached != null && !cached.isEmpty()) {
        return new ArrayList<>(cached);
    }
    // 2. 缓存没有，查 ZK
    return refreshAndGet(serviceName);
}

// Consumer 启动时调用一次
public void watch(String serviceName) {
    CuratorCache cache = CuratorCache.build(client, path);
    cache.listenable().addListener((type, oldData, newData) -> {
        // 服务变化时刷新缓存
        refreshAndGet(serviceName);
    });
    cache.start();
}
```
# 五、设计决策4：为什么需要优雅停机？

先看一个场景
```text
Provide 进程被 kill
	│
    ▼
zk临时节点还在（要等会话超时）
	│
    ▼
consumer从缓存里拿一个"已死"的地址
	│
    ▼
调用失败
	│
    ▼
等30秒（zk会话超时）后才感知
```

这就是问题：临时节点解决了"进程死了自动摘除"，但摘除需要等会话超时（默认30秒）
优雅停机解决的：进程主动下线时，立刻通知zk删除节点。
实现：
```java 
public class ShutdownHook {
    public static void register(Runnable cleanup) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                cleanup.run();
            } catch (Exception e) {
                log.error("优雅停机失败", e);
            }
        }, "rpc-shutdown-hook"));
    }
}
// Provider 启动时
ShutdownHook.register(() -> {
    registry.unregister(HelloService.class.getName(), serviceAddress);
});
```
对比

| 场景      | 无优雅停机          | 有优雅停机          |
| ------- | -------------- | -------------- |
| kill -9 | zk会话超时后摘除（30s） | 无差别（-9不执行hook） |
| kill    | 同上             | 立即注销节点         |
| 正常退出    | 同上             | 立即注销节点         |
# 六、完整链路：服务注册与发现
```text
[Provider 启动]
   │
   ▼
Netty Server 绑定端口（先启动，后注册）
   │
   ▼
创建 ZK 持久节点：/rpc/com.example.HelloService
   │
   ▼
创建 ZK 临时节点：/rpc/com.example.HelloService/192.168.1.100:8080
   │
   ▼
注册 ShutdownHook
   │
   ▼
开始服务
[Consumer 启动]
   │
   ▼
创建 ZK 连接
   │
   ▼
watch(服务名) → 建立 CuratorCache 监听
   │
   ▼
discover(服务名) → 从 ZK 拉取地址列表 → 写入本地缓存
   │
   ▼
代理调用时：从缓存拿地址 → 负载均衡 → 选一个 → 调用
[Provider 挂掉]
   │
   ▼
ShutdownHook 触发（或会话超时）
   │
   ▼
ZK 删除临时节点
   │
   ▼
Consumer 的 CuratorCache 收到通知
   │
   ▼
刷新本地缓存
   │
   ▼
下次调用只用存活的地址
```

# 七、问题记录
* **ZK 连接失败但程序不报错**
	* 
* **Consumer 的 Watcher 没触发**


* **Provider 端口冲突但注册成功了**


* **`CuratorCache` 导致重复监听**
	* 现象：Consumner频繁刷新，日志爆炸
	* 原因：watch()被调用了多次（比如多次创建Consumner实例）
	* 解决：watch()只调用一次，或者用Map<serverName,CuratorCache>去重
