[[Ⅵ、负载均衡、熔断、重试-三驾马车怎么配合]] 跑通了，但用起来很麻烦
每次调用都需要
```java
ZkServiceDiscovery discovery = new ZkServiceDiscovery("127.0.0.1:2181");
RpcClientProxy proxy = new RpcClientProxy(discovery);
HelloService helloService = proxy.getProxy(HelloService.class);
```

三行代码，才能拿到一个可用的服务。如果业务有10个服务，那么就需要写这样的代码20行
再本阶段做了两件事：
* 把调用链路将清楚- 一次调用到底经历了什么。
* 把框架变成产品- 从getProxy到@RpcReference


# 一、讲清楚：一次调用的11步链路
```text
[Consumer 业务代码]
  helloService.sayHello("rpc")
      │
      ▼
┌─────────────────────────────────────────┐
│ 1. 动态代理拦截（RpcClientProxy）        │
│    - 构建 RpcRequest                    │
│    - 生成 requestId                     │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 2. 调用拦截器链                          │
│    - 日志拦截器                         │
│    - 耗时统计拦截器                      │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 3. 服务发现（ServiceDiscovery）          │
│    - 查本地缓存                         │
│    - 缓存没有 → 查 ZK                   │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 4. 容错层（FailoverCluster）             │
│    - 黑名单过滤                         │
│    - 熔断过滤                           │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 5. 负载均衡（LoadBalancer）              │
│    - 选一个地址                         │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 6. 协议组装 + 序列化                     │
│    - RpcMessage{requestId, codec, ...}  │
│    - Serializer.serialize()             │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 7. Netty 发送                            │
│    - pendingRequests.put(requestId, f)  │
│    - channel.writeAndFlush()            │
└─────────────────────────────────────────┘
      │
      ══════ TCP 字节流 ══════
      │
┌─────────────────────────────────────────┐
│ 8. 服务端解码 + 反序列化                  │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 9. 反射调用本地服务                       │
│    - serviceMap.get(interfaceName)      │
│    - method.invoke()                    │
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│ 10. 组装响应 + 序列化 + 发送              │
└─────────────────────────────────────────┘
      │
      ══════ TCP 字节流 ══════
      │
┌─────────────────────────────────────────┐
│ 11. 客户端解码 + requestId 匹配           │
│    - pendingRequests.remove(id)         │
│    - future.complete(response)          │
└─────────────────────────────────────────┘
      │
      ▼
[返回给业务代码]
```
这张图就是RPC的全部。

# 二、设计决策1：为什么需要加拦截器
问题：日志、耗时、参数校验这些"横切关注点"，如果散落再代码层，代码会很混乱
没有拦截器
```java
public Object invoke(...) {
    log.info("调用开始");
    long start = System.currentTimeMillis();
    try {
        Object result = doInvoke(...);
        log.info("调用成功，耗时 {}", System.currentTimeMillis() - start);
        return result;
    } catch (Exception e) {
        log.error("调用失败", e);
        throw e;
    }
}
```
问题：
* 日志、耗时、校验、监控全堆再一起
* 想加一个鉴权，又要改代理层代码
* 关注点没有分离

有拦截器后
```java
public Object invoke(...) {
    interceptorChain.before(request);
    try {
        Object result = doInvoke(...);
        interceptorChain.after(request, response);
        return result;
    } catch (Exception e) {
        interceptorChain.onException(request, e);
        throw e;
    }
}
```

每个关注点一个拦截器，通过order排序
和Spring AOP的对比

| 方案         | 优点           | 缺点          |
| ---------- | ------------ | ----------- |
| Spring AOP | 生态好，注解驱动     | 依赖Spring ,重 |
| 自定义拦截器     | 轻量、不依赖spring | 需要自己实现      |
自定义拦截器：因为RPC框架的拦截器需求简单，不需要Spring AOP那么强大的表达式和切点

# 三、设计决策2：为什么共用`RpcConText+`ThreadLocal`?
场景：拦截器之间需要信息共享（比如requestId，开始时间、远程地址）
方案对比

| 方案          | 问题              |
| ----------- | --------------- |
| 方法参数传递      | 每个拦截器都要加参数，接口爆炸 |
| 全局变量        | 并发不安全           |
| ThreadLocal | 线程隔离，天然支持       |
实现：
```java 
public class RpcContext {
    private static final ThreadLocal<RpcContext> LOCAL =
        ThreadLocal.withInitial(RpcContext::new);
    public static RpcContext get() { return LOCAL.get(); }
    public static void remove() { LOCAL.remove(); }
}
```
关键点：必须再`finally`里面`remove()`,否则线程复用时造成内存泄漏
```java
try {
    // 使用 RpcContext
} finally {
    RpcContext.remove();  // 必须清理
}
```

# 四、设计决策3：为什么要做注解驱动？
开头也提到，一个服务需要写3行代码，创建`ZkServiceDiscovery`是重复的；业务代码里混入了 框架代码
使用注解驱动的代码
```java
@RpcReference
private HelloService helloService;
```

一行注解，搞定一切。
为什么能做到：`BeanPostProcessor`再Beanchu'sh