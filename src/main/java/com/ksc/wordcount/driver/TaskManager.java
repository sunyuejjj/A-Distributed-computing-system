package com.ksc.wordcount.driver;

import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.TaskContext;
import com.ksc.wordcount.task.TaskStatus;
import com.ksc.wordcount.task.TaskStatusEnum;
import scala.Int;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class TaskManager {

    /**
     * stageId和task队列的映射
     */
    private Map<Integer,BlockingQueue<TaskContext>> stageIdToBlockingQueueMap = new HashMap<>();

    /**
     * stageId和taskId的映射
     */
    private Map<Integer, List<Integer>> stageMap = new HashMap<>();

    /**
     * taskId和task状态的映射
     */
    Map<Integer, TaskStatus> taskStatusMap = new HashMap<>();


    public BlockingQueue<TaskContext> getBlockingQueue(int stageId) {
        return stageIdToBlockingQueueMap.get(stageId);
    }

    public void registerBlockingQueue(int stageId,BlockingQueue blockingQueue) {
        stageIdToBlockingQueueMap.put(stageId,blockingQueue);
    }

    public void addTaskContext(int stageId, TaskContext taskContext) {
        //建立stageId和任务的映射
        getBlockingQueue(stageId).offer(taskContext);  // blockingQueue -> stageId : taskList
        if(stageMap.get(stageId) == null){
            stageMap.put(stageId, new ArrayList());
        }
        //建立stageId和任务Id的映射
        stageMap.get(stageId).add(taskContext.getTaskId()); // stageMap -> stageId : taskId
    }


    public StageStatusEnum  getStageTaskStatus(int stageId){
        //todo 学生实现 获取指定stage的执行状态，如果该stage下的所有task均执行成功，返回FINISHED
        // 就是挨个去查stageId对应的所有taskId
        // 再根据taskId去查task
        for(Integer taskId : stageMap.get(stageId)){
            if(taskStatusMap.get(taskId) == null){
                return StageStatusEnum.RUNNING;
            }
            if(taskStatusMap.get(taskId).getTaskStatus() == TaskStatusEnum.RUNNING){
                return StageStatusEnum.RUNNING;
            }
            if(taskStatusMap.get(taskId).getTaskStatus() == TaskStatusEnum.FAILED){
                return StageStatusEnum.FAILED;
            }
            if(taskStatusMap.get(taskId).getTaskStatus() == TaskStatusEnum.FINISHED){
                continue; // 需要确保每一个任务都finish了才能返回finish
            }
        }
        return StageStatusEnum.FINISHED;
    }

    public ShuffleBlockId[] getStageShuffleIdByReduceId(int stageId,int reduceId){

        List<ShuffleBlockId> shuffleBlockIds = new ArrayList<>();
        // 更具stage获得所有的taskId
        //
        for(int taskId:stageMap.get(stageId)){
            // 按照reduceId对shuffle的结果进行分配
            ShuffleBlockId shuffleBlockId = ((MapStatus) taskStatusMap.get(taskId)).getShuffleBlockIds()[reduceId];
            shuffleBlockIds.add(shuffleBlockId);
        }
        return shuffleBlockIds.toArray(new ShuffleBlockId[shuffleBlockIds.size()]);
    }

    public void updateTaskStatus(TaskStatus taskStatus) {
        taskStatusMap.put(taskStatus.getTaskId(),taskStatus); // taskId与task状态的映射
    }


    private int maxTaskId = 0;

    public int generateTaskId() {
        return maxTaskId++;
    }


}
