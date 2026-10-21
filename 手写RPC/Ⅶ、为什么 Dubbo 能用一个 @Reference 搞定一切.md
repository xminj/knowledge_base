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
为什么能做到：`BeanPostProcessor`再Bean初始化前，扫描字段上的`@RpcReference`，注入动态代理。

# 五、设计决策4：为什么使用`BeanPostProcessor`注入代理？

| 方案                     | 优点        | 缺点            |
| ---------------------- | --------- | ------------- |
| 手动`getProxy()`         | 简单        | 每次都要写         |
| `@Autowired` + `@Bean` | Spring 原生 | 需要为每个接口声明Bean |
| `BeanPostProcessor`    | 自动，五侵入    | 需要反射          |
选择`BeanPostProcessor`：它是Spring提供的Bean初始化前后钩子，再Bean初始化之前把代理塞进去，用户完全无感知。
实现：
```java
@Override
public Object postProcessBeforeInitialization(Object bean, String beanName) {
    for (Field field : bean.getClass().getDeclaredFields()) {
        if (!field.isAnnotationPresent(RpcReference.class)) continue;
        field.setAccessible(true);
        field.set(bean, proxy.getProxy(field.getType()));
    }
    return bean;
}
```

# 六、设计决策5：为什么用 `SmartInitializingSingleton` 而不是 `@PostConstruct`？
Provide端需要再启动时注册服务。

| 钩子                           | 时机            | 问题               |
| ---------------------------- | ------------- | ---------------- |
| `@PostConstruct`             | 单个Bean初始化     | 太早，其他Bean可能还没有就绪 |
| `ApplicationRunner`          | 启动完成前         | 太晚，服务已经暴露        |
| `SmartInitializingSingleton` | 所有单例Bean初始化后， | 刚好               |

为什么`@PostConstruct`不行
* `@PostConstruct`在当前Bean初始化后执行
* 此时其他Bean可能还没有创建
* 如果`RpcServerRunner`依赖`ServiceRegistry`，可能拿到null
为什么`ApplicationRunner`不行
* `ApplicationRunner`在Spring Boot启动最后执行
* 此时Timcat已经监听端口了，服务已经暴露
* 如果Netty启动失败，用户可能已经调用了。

`SmartInitializingSingleton`的时机：
* 所有单例Bean初始化完毕
* 但Spring boot还没完全启动
* 这时的依赖都就绪了，但还没有对外服务的完美瞬间


# 七、设计决策6：为什么 `@RpcService` 要加 `@Component`？
问题：如果`@RpcService`不加`@Component`，标注的类不会被扫描为Spring Bean。`getBeansWithAnnotation(RpcService.class)` 找不到它。
解决：`@RpcService` 上再加 `@Component`：
```java
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component                    // ← 关键
public @interface RpcService {
    Class<?> interfaceClass() default void.class;
}
```
**这是 Spring 的"元注解"技巧**：`@Component` 是 `@RpcService` 的元注解，标注 `@RpcService` 等价于标注 `@Component`。


# 九、问题记录
* **`@RpcReference` 注入后还是 null**
	* 现象：Consumer 启动正常，调用时字段为 null。
	* 原因：`BeanPostProcessor` 的 `postProcessBeforeInitialization` 在**构造器之后**执行。如果字段在构造器里被使用，此时还是 null。
	* 解决：不要在构造器里使用 `@RpcReference` 字段，用字段注入或 setter 注入。
	* 教训：**Spring 的依赖注入有严格的顺序，构造器早于所有后处理器。**


* **Provider 注册的地址 Consumer 访问不到**
	* 现象：ZK 里能看到节点，Consumer 也能发现，但调用超时。
	* 原因：`InetAddress.getLocalHost().getHostAddress()` 返回的是 `127.0.0.1`，跨机器访问不到。
	* 解决：用 `NetworkInterface` 遍历网卡，取第一个非回环的 IPv4 地址。

 
* **Spring Boot 3.x / 4.x 找不到自动配置**
	* 现象：加了 starter 依赖，但 `RpcAutoConfiguration` 没生效。
	* 原因：Spring Boot 3.x 移除了 `spring.factories` 对自动配置的支持，改用 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`。
	* 解决：两个文件都放，兼容 2.x / 3.x / 4.x。
	* 教训：**框架的自动配置机制在演进，踩坑是学习的最好机会。**


* **拦截器顺序错乱**
	* 现象：耗时统计的日志出现在日志拦截器之前，耗时数据不准。
	* 原因：`InterceptorChain` 没有排序。
	* 解决：`addInterceptor` 时按 `order` 排序，`before` 正序执行，`after` 逆序执行（像 AOP 的环绕通知）。
	* 教训：**拦截器链的顺序，决定了日志的因果逻辑。**
 
* **`UnsupportedOperationException` —— 不可变集合的坑**
	* 现象：调用时抛 `UnsupportedOperationException`，堆栈指向 `available.remove(address)`。
	* 原因：`filter` 方法返回的 `List` 是不可变的（`Stream.toList()` 在 JDK 16+ 返回不可变列表），`remove` 直接抛异常。
	* 解决：`new ArrayList<>(filter(...))` 包一层，或者让 `filter` 直接返回可变列表。
	* 教训：**任何要 `remove` 的 `List`，都必须确认它是可变的。**