package com.ksc.wordcount.rpc.Driver;

import akka.actor.AbstractActor;
import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.rpc.ExecutorRegister;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

public class DriverActor extends AbstractActor {

    @Override
    public Receive createReceive() {
        // 分配不同的任务到不容的map
        //
        return receiveBuilder()
                .match(TaskStatus.class, mapStatus -> {
                    System.out.println("ExecutorActor received mapStatus:"+mapStatus);
                    if(mapStatus.getTaskStatus() == TaskStatusEnum.FAILED) {
                        System.err.println("task status taskId:"+mapStatus.getTaskId());
                        System.err.println("task status errorMsg:"+mapStatus.getErrorMsg());
                        System.err.println("task status errorStackTrace:\n"+mapStatus.getErrorStackTrace());
                    }
                    // 执行任务
                    DriverEnv.taskManager.updateTaskStatus(mapStatus); // 更新任务状态
                    DriverEnv.taskScheduler.updateTaskStatus(mapStatus); // 更新处理器状态
                })
                .match(ExecutorRegister.class, executorRegister -> {
                    System.out.println("ExecutorActor received executorRegister:"+executorRegister);
                    DriverEnv.executorManager.updateExecutorRegister(executorRegister);
                })
                .match(Object.class, message -> {
                    //处理不了的消息
                    System.err.println("unhandled message:" + message);
                })
                .build();
    }
}
