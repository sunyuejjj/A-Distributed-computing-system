package com.ksc.wordcount.rpc.Driver;

import akka.actor.ActorRef;
import com.ksc.wordcount.task.TaskContext;

public class DriverRpc {

    public static void submit(String executorUrl,TaskContext taskContext){
        // 远程调用rpc
        System.out.println("DriverRpc submit executorUrl:"+executorUrl+",taskContext:"+taskContext);
        DriverSystem.getExecutorRef(executorUrl).tell(taskContext, ActorRef.noSender());
    }
}
