# 引入 ： RPC到底难在哪里？

提到RPC，很多人第一反应是Dubbo、gRPC、Thrift。它们功能强大，但源码动辄几十万行，初学者一进去就迷路。

**RPC最核心的那个`魔法`，到底是怎么实现的？** 于是我给自己定了个小目标-**不引入Netty，不引入任何序列号框架**，只用JDK自带的Socket+动态代理+反射，跑通一次远程调用。

结果：**150行代码，一次调用从客户端到服务端再回来，全链路跑通**

这篇文章记录这个过程，以及我在每一步的设计思考。

# 一、先拆解：RPC到底要解决什么？

我把`本地调用`和`RPC调用` 摆在一起对比

```java
// 本地调用
String result = helloService.sayHello("rpc")

// RPC 调用  希望看起来要和本地调用一样
String result  = helloSerive.sayHello("rpc")
```

对调用方来说，**这两行代码必须长的一样**。但背后，RPC要偷偷完成这些事。

```text
1、找到服务地址（服务发现）
2、建立网络链接（通信）
3、把方法名、参数变成字节（序列化）
4、发过去（网络传输）
5、服务端反序列化、找到对应方法、执行（反射）
6、结果原路返回
7、把结果塞回调用发
```

**核心矛盾**：调用发只想写一行普通的方法调用，但这行代码背后要发生7件事。

**RPC的第一性原理**：如何让`远程`看起来像`本地`？ --- 答案便是：动态代理

# 二、设计决策1：为什么是动态代理？

要让`helloService.sayHello("rpc")` 看起来像本地调用，就必须在`调用`和`执行`之间插一层拦截。

**候选方案对比**

| 方案      | 做法                       | 优点       | 缺点           |
| ------- | ------------------------ | -------- | ------------ |
| 手写Stub  | 每个接口都需要编写一个实现类           | 直观、无反射   | 每个接口都要写、无法通用 |
| JDK动态代理 | `Proxy.newProxyInstance` | 无侵入、接口透明 | 只能代理接口       |
| CGLIB   | 字节码生成子类                  | 可代理      | 依赖字节码库       |
**我的选择：JDK动态代理**

**理由**

* RPC场景下服务都以interface暴露，JDK代理够用
* 零依赖，不引入任何字节码库
* 后续要代理类，可以换成Javassist（Dubbo就是这么做的）

**代价**
* 只能代理接口
* 反射调用有性能损耗（后面用Method缓存优化）

**核心代码**

```java
public static <T> T  getProxy(Class<T> interfaceClass,String hot,int port){
	return (T)Proxy.newProxyInstance(
		interfaceClass.getClassLoader,
		new Class[]{interfaceClass},
		(proxy,method,ages) -> {
			// 这里就是"魔法"发生的地方
			// 本地调用被拦截，换成网络请求
		}
	);
}
```
**思考**
 > 动态代理的本质是*欺骗*，调用方以为自己调用的是本地方法，其实被*InvocationHandler*拦截，换成了网络请求，RPC的透明性就建立在这个*谎言*上。

# 三、设计决策2：为什么先使用Socket，不直接上Netty？

引入Netty，比较容易被其Netty中的`EvneLoop`、`ChannelPipeline`、粘包拆包搞晕。主要的目的是学习RPC的并不是Netty，很容易忘记自己在做什么。

**先用最裸的Socket跑通链路，再逐步替换**

| 维度   | Socket（BIO） | Netty |
| ---- | ----------- | ----- |
| 学习成本 | 极低          | 高     |
| 并发能力 | 一线程一连接      | 高并发   |
| 粘包处理 | 自己写         | 框架提供  |
| 适合阶段 | 验证核心链路      | 生产级   |
**为什么先Socket**
* 核心链路（代理 -> 序列化 ->传输 -> 反射 -> 返回）和通信框架无关
* 用Socket能最快验证*理解的是否正确*
* 后面换netty时，只是替换通讯层，其他代码不动

**代价**
* BIO无法支撑高并发
* 没有心跳、重连机制

**思考**
>工程上最容易犯的错，是过早优化。RPC的核心是*透明调用*这个抽象，不是*高性能通信*。先证明抽象成立，在谈性能

# 四、设计决策3：请求/响应对象怎么设计？

网络传输不能直接传**方法调用**，必须把它变成一个可序列化的对象

**设计**
```java
public class RpcResuest implements Serializable {  
  
    /**  
     * 接口名称  
     */  
    private String interfaceName;  
  
    /**  
     * 方法名称  
     */  
    private String methodName;  
  
    /**  
     * 方法参数类型  
     */  
    private Class<?>[] paramTypes;  
  
    /**  
     * 方法参数  
     */  
    private Object[] args;   
}


public class RpcResponse implements Serializable {  
  
    /**  
     * 相应数据  
     */  
    private Object data;  
  
    /**  
     * 状态信息
     */  
    private String message;  
  
}
```

**为什么这么设计：**

| 字段              | 为什么需要                       |
| --------------- | --------------------------- |
| `interfaceName` | 服务端靠它找到接口实现                 |
| `methodName`    | 找到具体的方法                     |
| `parmTypes`     | 反射`getMethod` 必须传类型（重载方法区分） |
| `args`          | 实际参数                        |

**注意**：一开始以为`methodName`就够了，结果遇到重载方法时`getMethod`抛`NoSuchMethodException`异常，**参数类型时必须的**

**响应设计**

* `data`:正常返回
* `meeesg` ：区分成功/失败（先阶段使用字符串0，后面回换成异常对象）

**思考**

> 请求对象RPC的信使，它必须携带足够的信息，让服务端在不了解客户端任何上下文的情况下，也能完成调用。这就是所谓的**自描述**

# 五、设计决策4：服务端怎么找到并执行方法

客户端发过来的是**RpcRequest**，服务端得知道**这该调用那个类的那个方法**

**核心机制：服务注册表+反射**
```java
// 服务注册表
private final Map<String, Object> serviceMap = new HashMap<>();

// 注册
public void register(String interfaceName, Object service) {  
    serviceMap.put(interfaceName, service);  
}

// 执行
RpcResuest resuest = (RpcResuest) input.readObject();  
Object service = serviceMap.get(resuest.getInterfaceName());  
  
Method method = service.getClass().getMethod(resuest.getMethodName(), resuest.getParamTypes());  
  
Object result = method.invoke(service, resuest.getArgs());
```

**为什么使用Map**
* `interfaceName -> 实现实例`
* 0(1)查找
* 简单直接

**为什么使用范式**
* 服务端不知道客户端会调用那个方法
* 反射是*运行时动态调用* 的唯一手段
**代价**
* 反射比直接调用慢（后面会换成Method）
* 每次都要*getMethod*（后面会优化）

**思考**
>反射是RPC服务端的*万能钥匙*。客户端说*调getMonth(int)*，服务端不需要预编译任何东西，靠反射就能找到并执行。这是Java反射最经典的应用场景

# 六、完整链路：一次调用的全过程
```text
[客户端]
	helloService.sayHello("rpc")
		|
		▼
	JDK 动态代理拦截
		| 构建 RpcRequest{HelloService, sayHello, [String], ["rpc"]}
		▼
	Socket 连接 Server
		| ObjectOutputStream 序列化
		══════ 网络 ══════
		|
[服务端]
	ServerSocket 接收
		|ObjectInputStream 反序列化
		▼
	serviceMap.get("HelloService") → HelloServiceImpl
		|
		▼
	method = getMethod("sayHello", String.class)
		|
		▼
	method.invoke(service, "rpc") → "hello rpc"
		|
		▼
	构建 RpcResponse{"hello rpc", "ok"}
		| 序列化
		══════ 网络 ══════
		|
[客户端]
	反序列化，返回 data
		|
		▼
	"hello rpc"
```

# 七、问题记录

* 反序列化顺序错误导致卡死
	* 现象：客户端发送后永远卡在*readObject*
	* 原因：*ObjectOutputStream* 必须在*ObjectInputStream*之前创建，否则两端等待对方的流头
	* 解决：服务端先创建*ObjectOutpStream*在创建*ObjectInputStream*，或者两端都先flush流头
	* socket流创建顺序是有讲究的，序列化流尤其如此
* 服务端方法找不到
	* 现象：*NoSuchMethodException: sayHello()*
	* 原因：*getMethod*需要参数类型，最初没传*paramTypes*
	* 解决：*RpcRequest*里加上*Class<>[] paramTypes* 
* 异常被吞
	* 现象：服务端抛异常。客户端只能看到超时
	* 解决：服务端*catch*后把异常信息放进*RpcResponse.message*,客户端读到后抛出