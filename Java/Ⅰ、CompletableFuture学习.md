**CompletableFuture** ：核心是两个字段和一套"栈堆"的触发回调机制
# 一、两个核心字段：状态与依赖

**CompletableFuture** 内部只有两个关键的**volatile** 字段
```java
volatile Object result;       // Either the result or boxed AltResult  -- 计算结果或异常包装
volatile Completion stack;    // Top of Treiber stack of dependent actions -- 依赖动作栈（Treiber Stack）
```

`result`决定这个Future是否*完成*。`null`表示未完成；非`null`表示已完成。正常返回值直接存入；返回`null`或抛出异常时，用一个内部类`AltResult`包装。`AltResult.ex == null`代表正常返回了`null`，`AltResult.ex != null`代表异常。所有的`get()`、`join()`、`isDone()`的判断，底层都是在看这个字段是否为空。

`stack`是一个用CAS维护的无锁栈(Treiber stack)，栈里存放的时`Completion`节点，每一个`thenApply`、`thenAccept`、`whenComplete`调用，都会生成一个`complete`子类实例，压入当前Future的`stack`中。你可以把它理解成"完成通知单"：当前Future一旦完成，就会遍历这个栈，逐个触发登记过的后续动作。

# 二、链式调用的本质：Completion节点串联

当`cf.thenApply(fn)`时，源码做的大致是：
* 创建一个新的`CompletableFuture`(记为d)，作为`thenApply`的返回结果。
* 创建一个`UniApply`节点，它持有：源Future `this`、下游Future 