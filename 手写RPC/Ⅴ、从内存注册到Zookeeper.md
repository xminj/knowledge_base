
[[Ⅳ、SPI 可插拔 + 三种序列化实现]]跑通了序列化，但服务注册地址都写死在内存Map中。跨进程无法发现注册的服务地址
```java
// Consumer 里手动塞地址
SimpleServiceRegistry registry = new SimpleServiceRegistry();
registry.register(HelloService.class.getName(), "127.0.0.1:8080");
```
问题很明显：
* 跨进程不可用：Prodiver注册在它自己的进程里，Consumer根本看不到
* Provider挂了不知道：Consumer一致调用一个死地址
* 无法动态扩容：