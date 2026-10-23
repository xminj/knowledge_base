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
* 创建一个`UniApply`节点，它持有：源Future `this`、下游Future `d`、函数`fn`
* 如果`this`已完成，直接在当前线程执行`fn`，把结果填入`d`,返回`d`
* 如果`this`未完成，把`UniApply`节点压入`this.stack`，然后返回那个还未完成的`d`

关键点在于：`thenApply`**返回的`d`本身可能还没有完成**。调用方拿到`d`后继续`.thenAppley(....)`,就是在`d`的`stack`里再压一个节点。这样，多个Future通过`Completion`节点里`dep`（下游引用）和`src`（上游引入）指针，形成一条跨多个Future的链。

# 三、完成的传播：postComplete与tryFire

当某个Future被`complete(value)`时，源码不会直接”通知“所有下游，而是做两件事
* CAS把`result`从`null`改为`value`（或`AltResult`）
* 调用`postComplete()`，开始弹栈并触发
`postComplete`的逻辑是：从`stack`顶部不断弹出`Complete`节点，对每个节点调用它的`tryFire()`方法，`tryFire`会检查源Future是否真正的完成了，如果完成，就会执行传入的函数，然后把结果填入下游Future `d` ，并递归触发`d`自己的`postComplete`,把完成信号继续向下传播。

这就是为什么链式调用的”惰性“的：注册只是压栈，真正只执行发成再源Future被完成的那一刻，由完成线程一路推下去。


# 四、并发安全怎么保证

整个结构没有用`synchronized`，全靠CAS
* 压栈用`tryPushStack`，CAS替换`stack`头节点，失败就自旋重试
* 完成结果用`internalComplete`，CAS把`result`从`null`改为目标值，保证只有一个线程能”成功完成“它。
* 触发节点
`