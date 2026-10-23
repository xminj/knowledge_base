**CompletableFuture** ：核心是两个字段和一套"栈堆"的触发回调机制
# 一、两个核心字段：状态与依赖

**CompletableFuture** 内部只有两个关键的**volatile** 字段
```java
volatile Object result;       // Either the result or boxed AltResult  -- 计算结果或异常包装
volatile Completion stack;    // Top of Treiber stack of dependent actions -- 依赖动作栈（Treiber Stack）
```

`result`决定这个Future是否*完成*。`null`表示未完成；非`null`表示已完成。正常返回值直接存入；返回