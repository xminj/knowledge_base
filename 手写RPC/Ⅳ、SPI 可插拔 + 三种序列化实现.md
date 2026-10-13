
**引言：**  [[Ⅲ、从Socket到Netty]]  阶段Ⅲ已经跑通了，但字节太大。
阶段Ⅲ中使用的序列化时JDK原生的。
* 字节大：类描述信息、`serialVersionUID`、字段元数据全塞进去
* 性能差：反射+流式写入，慢的离谱
* 不跨语言：Python读不了java的字节流
* 不可读：调试的时候看到的是一堆乱码
* 有安全隐患：反序列化漏洞的常客

一个RPC框架，**序列化是数据传输的第一道关**。
在本次优化中将序列化做成了可插拔+多实现+可配置，并用压测数据说话

| 指标   | JDK        | Hessian    | Hessian快了多少          |
| ---- | ---------- | ---------- | -------------------- |
| 序列化  | 2791.49 ms | 1010.88 ms | **快约 2.76 倍**        |
| 反序列化 | 4765.89 ms | 2417.55 ms | **快约 1.97 倍**        |
| 字节数  | 4330       | 6539       | Hessian 大约 **大 51%** |

# 一、序列化在RPC里到底做什么？

RPC的一次调用，跨网络的只有字节，**序列化就是"对象 -> 字节"的翻译官**
```text
客户端：RpcRequest 对象 ──序列化──> byte[] ──网络──> 服务端
服务端：byte[] ──反序列化──> RpcRequest 对象
```
**它要满足四个要求**

| 要求  | 说明             |
| --- | -------------- |
| 正确性 | 序列化再反序列化，对象要一致 |
| 性能  | 序列化和反序列化要快     |
| 体积  | 字节数要小（网络传输成本）  |
| 兼容  | 字段增减不影响老客户端    |

# 二、设计决策1：为什么序列化要做成SPI？
在阶段Ⅲ中序列化实现
```java
static {
    register(new JdkSerializer());
    register(new JsonSerializer());
}
```
**问题很明显**
* 写死了所有实现，加一个Hessian要改工厂
* 全部提前new一遍，不用的也创建了
* 第三方想扩展，改不到我的源码

**候选方案**

| 方案               | 优点     | 缺点          |
| ---------------- | ------ | ----------- |
| 静态方案             | 简单     | 写死，不可扩展     |
| Java SPI         | jdk内置  | 全量加载，无法按名获取 |
| Dubbo SPI        | 按需，有别名 | 要自己实现       |
| Spring Factories | 生态好    | 依赖Spring    |

**选择Dubbo风格的轻量SPI**
核心机制
```text
配置文件：META-INF/rpc/com.minirpc.common.Serializer
内容：
  jdk=com.minirpc.serialization.JdkSerializer
  json=com.minirpc.serialization.JsonSerializer
  hessian=com.minirpc.serialization.HessianSerializer
```
加载时
```java
Serializer s = SpiLoader.load(Serializer.class, "hessian");
```
**为什么不用Java SPI**
* Java SPI 一次性加载说所有实现，不管用不用
* 无法按名字获取指定的实现，只能遍历
* 没有默认值、别名、优先级

# 三、SPI实现
**@SPI注解**
```java
@Documented  
@Target(ElementType.TYPE)  
@Retention(RetentionPolicy.RUNTIME)  
public @interface SerializerSPI {  
    /**  
     * 默认实现名  
     */  
    String value() default "";  
}
```

**接口声明**
```java 
/**  
 * 序列化接口 —— 所有序列化实现必须遵守  
 */  
@SerializerSPI("hessian")  
public interface Serializer {  
    /**  
     * 序列化  
     *  
     * @param obj 参数  
     * @return 序列化后的字节数组  
     */  
    byte[] serialize(Object obj);  
  
    /**  
     * 反序列化  
     *  
     * @param bytes 数据  
     * @param clazz 对象  
     * @param <T>   反序列化后的对象  
     * @return T  
     */    <T> T deserialize(byte[] bytes, Class<T> clazz);  
  
  
    /**  
     * 序列化类型编码（用于协议头标识）  
     */  
    byte getCodec();  
}
```

**SpiLoader核心逻辑**
```java
public class SerializerSpiLoader {  
  
    /**  
     * 配置目录  
     */  
    private static final String SPI_DIR = "META-INF/rpc/";  
  
    /**  
     * 缓存，接口类 -> (name -> 实例)  
     */    private static final Map<Class<?>, Map<String, Object>> serializers = new ConcurrentHashMap<>();  
  
  
    /**  
     * 按名字加载  
     */  
    @SuppressWarnings("unchecked")  
    public static <T> T load(Class<T> clazz, String name) {  
        Map<String, Object> instances = serializers.computeIfAbsent(clazz, SerializerSpiLoader::loadAll);  
        Object instance = instances.get(name);  
        if (Objects.isNull(instance)) {  
            throw new RpcException("未找到 SPI 实现: " + clazz.getName() + "#" + name);  
        }  
        return (T) instance;  
    }  
  
  
    /**  
     * 加载默认是实现  
     */  
    public static <T> T loadDefault(Class<T> clazz) {  
        SerializerSPI spi = clazz.getAnnotation(SerializerSPI.class);  
        if (Objects.isNull(spi) || spi.value().isEmpty()) {  
            throw new RpcException("接口未标注 @SPI 或未指定默认实现: " + clazz.getName());  
        }  
        return load(clazz, spi.value());  
    }  
  
    /**  
     * 加载某接口的全部实现  
     */  
    public static Map<String, Object> loadAll(Class<?> clazz) {  
        Map<String, Object> result = new ConcurrentHashMap<>();  
        String resource = SPI_DIR + clazz.getName();  
  
        try {  
            ClassLoader cl = SerializerSpiLoader.class.getClassLoader();  
            Enumeration<URL> urls = cl.getResources(resource);  
            while (urls.hasMoreElements()) {  
                URL url = urls.nextElement();  
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) {  
                    String line;  
                    while ((line = reader.readLine()) != null) {  
                        line = line.trim();  
                        // 增加 注释过滤  
                        if (line.isEmpty() || line.startsWith("#")) continue;  
  
                        int idx = line.indexOf('=');  
                        if (idx < 0) continue;  
  
                        String name = line.substring(0, idx).trim();  
                        String className = line.substring(idx + 1).trim();  
  
                        Class<?> implClass = Class.forName(className);  
                        Object instance = implClass.getDeclaredConstructor().newInstance();  
  
                        result.put(name, instance);  
                    }  
                }  
            }  
  
        } catch (Exception e) {  
            throw new RpcException("加载 SPI 失败: " + clazz.getName(), e);  
        }  
        return result;  
    }
```

**设计细节**
* serializers缓存：避免每次都读文件
* getResources：支持多个jar提供同一个接口的实现
* 格式name=class：比Java SPI的一行一个类更灵活

# 四、设计决策2：为什么选Hessian2做默认？

候选方案对比

| 方案    | 性能  | 跨语言 | 可读  | 复杂度 |
| ----- | --- | --- | --- | --- |
| JDK原生 |     |     |     |     |
|       |     |     |     |     |
|       |     |     |     |     |
|       |     |     |     |     |
