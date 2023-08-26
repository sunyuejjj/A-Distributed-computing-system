# 一个分布式计算框架



## 项目描述
仿照MapReduce实现了一个简易分布式计算框架，计算网站URL访问频率的排名，分为Driver和Executor两部
分，Driver用于任务的调度，Executor用于实际的计算。使用Netty和Akka两种RPC框架进行网络通讯，实现的分布式计
算框架具有可拓展性，支持分布式启动多个Executor，支持任务的拓展。
## 项目要点
1. Driver实现：读取URL日志文件，将文件切分成Split，按照Split分区创建Map任务，启动Akka服务，收到Executor的
注册信息后，将Map任务分发给Executor执行，待Map任务全部执行完毕，对于Reduce任务执行同样操作。
2. Executor实现：启动Akka服务向Driver完成注册并接受任务分配，启动Netty服务用于后续通过网络读取shuffle中间文
件，在Map任务阶段完成计算任务后将中间文件落盘，后续Reduce任务阶段通过Netty服务读取中间文件计算并输出最终结
果。
## 应用技术
Java、Akka、Netty
