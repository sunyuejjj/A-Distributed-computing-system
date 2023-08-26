package com.ksc.wordcount.driver;

import com.ksc.wordcount.rpc.Driver.DriverRpc;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TaskScheduler {

    private TaskManager taskManager;
    private ExecutorManager executorManager ;

    /**
     * taskId和ExecutorUrl的映射
     */
    private Map<Integer,String> taskExecuotrMap=new HashMap<>();

    public TaskScheduler(TaskManager taskManager, ExecutorManager executorManager) {
        this.taskManager = taskManager;
        this.executorManager = executorManager;
    }

    public void submitTask(int stageId) {
        // 提交之后，将任务从阻塞队列中取出来，这里取出来的taskContent，也就是任务的具体内容
        BlockingQueue<TaskContext> taskQueue = taskManager.getBlockingQueue(stageId);

        while (!taskQueue.isEmpty()) {
            //todo 学生实现 轮询给各个executor派发任务
            executorManager.getExecutorAvailableCoresMap().forEach((executorUrl,availableCores)->{
                if(availableCores > 0 && !taskQueue.isEmpty()){
                    TaskContext taskContext = taskQueue.poll(); // 似乎也可以使用take()
                    // 建立任务Id和executorUrl 的映射
                    taskExecuotrMap.put(taskContext.getTaskId(),executorUrl);
                    executorManager.updateExecutorAvailableCores(executorUrl,-1); // 更新对应处理器的可用数量，即core数量
                    DriverRpc.submit(executorUrl,taskContext); // 向executor发送任务内容
                }
            });
            try {
                String executorAvailableCoresMapStr=executorManager.getExecutorAvailableCoresMap().toString();//ExecutorUrl和Core数的映射
                System.out.println("TaskScheduler submitTask stageId:"+stageId+",taskQueue size:"+taskQueue.size()+", executorAvailableCoresMap:" + executorAvailableCoresMapStr+ ",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void waitStageFinish(int stageId){
        StageStatusEnum stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        while (stageStatusEnum==StageStatusEnum.RUNNING){
            try {
                System.out.println("TaskScheduler waitStageFinish stageId:"+stageId+",sleep 1000");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stageStatusEnum = taskManager.getStageTaskStatus(stageId);
        }
        // todo： 可以添加重试
        if(stageStatusEnum == StageStatusEnum.FAILED){
            System.err.println("stageId:"+stageId+" failed");
            System.exit(1);
        }
    }

    public void updateTaskStatus(TaskStatus taskStatus){
        if(taskStatus.getTaskStatus().equals(TaskStatusEnum.FINISHED)||taskStatus.getTaskStatus().equals(TaskStatusEnum.FAILED)){
            String executorUrl=taskExecuotrMap.get(taskStatus.getTaskId());
            // 就是要更新driver这边executor的状态
            executorManager.updateExecutorAvailableCores(executorUrl,1);
        }
    }


}
