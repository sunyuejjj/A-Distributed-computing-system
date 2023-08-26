package com.ksc.wordcount.task;

import com.ksc.wordcount.rpc.Executor.ExecutorRpc;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

public abstract class Task<T>  implements Runnable {

    protected String applicationId;
    protected String stageId;
    protected int taskId;
    protected int partionId;

    public Task(TaskContext taskContext) {
        this.applicationId = taskContext.getApplicationId();
        this.stageId = taskContext.getStageId();
        this.taskId = taskContext.taskId;
        this.partionId = taskContext.getPartionId();
    }
    // 提交后运行这个方法，runnable
    public void run() {
        try{
            // 更新task的状态，实际上是更新driver端的task和executor 的状态
            ExecutorRpc.updateTaskMapStatue(new TaskStatus(taskId,TaskStatusEnum.RUNNING));
            // 运行runTask，这个由子类来实现
            TaskStatus taskStatus = runTask();
            // 得到任务的状态后，再次更新
            ExecutorRpc.updateTaskMapStatue(taskStatus);
        } catch (Exception e) {
            // 任务失败
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();
            System.err.println("task："+taskId+" failed：" );
            e.printStackTrace();
            TaskStatus taskStatus = new TaskStatus(taskId,TaskStatusEnum.FAILED,e.getMessage(),stackTrace);
            ExecutorRpc.updateTaskMapStatue(taskStatus);
            Thread.currentThread().interrupt();
        }
    }

    public abstract TaskStatus runTask() throws Exception;


}
