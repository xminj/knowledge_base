
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