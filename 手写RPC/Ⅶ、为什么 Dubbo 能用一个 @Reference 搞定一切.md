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

