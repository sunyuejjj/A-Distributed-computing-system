package com.ksc.wordcount.driver;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.ksc.wordcount.datasourceapi.FileFormat;
import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionWriter;
import com.ksc.wordcount.datasourceapi.UnsplitFileFormat;
import com.ksc.wordcount.rpc.Driver.DriverActor;
import com.ksc.wordcount.rpc.Driver.DriverSystem;
import com.ksc.wordcount.shuffle.ShuffleBlockId;
import com.ksc.wordcount.task.*;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapTaskContext;
import com.ksc.wordcount.task.reduce.ReduceFunction;
import com.ksc.wordcount.task.reduce.ReduceTaskContext;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class WordCountDriver {
    // driver，模仿spark
    public static void main(String[] args) {
        DriverEnv.host= "127.0.0.1";//这就一假的端口，实际上根本没有启动
        DriverEnv.port = 4040;
        String inputPath = "tmp/input";
        String outputPath = "tmp/output";
        String applicationId = "wordcount_001";
        int reduceTaskNum = 1;

        FileFormat fileFormat = new UnsplitFileFormat();
        //切分文件
        PartionFile[]  partionFiles = fileFormat.getSplits(inputPath, 200);

        TaskManager taskScheduler = DriverEnv.taskManager; // 获取任务管理器

        ActorSystem executorSystem = DriverSystem.getExecutorSystem(); //注册
        ActorRef driverActorRef = executorSystem.actorOf(Props.create(DriverActor.class), "driverActor");
        System.out.println("ServerActor started at: " + driverActorRef.path().toString());

        int mapStageId = 0 ;
        //添加stageId和任务的映射
        taskScheduler.registerBlockingQueue(mapStageId, new LinkedBlockingQueue());
        for (PartionFile partionFile : partionFiles) {
            MapFunction wordCountMapFunction = new MapFunction<String, KeyValue>() {

                @Override
                public Stream<KeyValue> map(Stream<String> stream) {
                    //todo 学生实现 定义maptask处理数据的规则,这里设置map执行的内容
                    Stream<KeyValue> res = stream.flatMap(line->{
//                        System.out.println("map任务，进来的是"+ line);
                        Stream<String> allStream = Stream.empty();
                        String url;
                        Pattern pattern = Pattern.compile("((http|https)?://[\\w.:/?=&]+)");
                        Matcher matcher = pattern.matcher(line);
                        while(matcher.find()){
                            url = matcher.group();
                            allStream = Stream.concat(allStream,Stream.of(url));
//                            System.out.println("map任务，出去的是" + url);
                        }
                        return allStream;
                    }).map(url-> new KeyValue(url,1));
                    return res;
                }

            };
            MapTaskContext mapTaskContext = new MapTaskContext(applicationId, "stage_"+mapStageId, taskScheduler.generateTaskId(),
                    partionFile.getPartionId(),
                    partionFile,// 文件分块信息
                    fileFormat.createReader(), // 怎么从文件里读
                    reduceTaskNum, // 因为需要根据reduce来划分task
                    wordCountMapFunction); // 读到的数据怎么进行map
            taskScheduler.addTaskContext(mapStageId,mapTaskContext);
        }

        //提交stageId
        DriverEnv.taskScheduler.submitTask(mapStageId); // 提交任务
        DriverEnv.taskScheduler.waitStageFinish(mapStageId); // 等待任务执行完毕

        // 做reduce
        int reduceStageId = 1 ;
        // 创建一个reduceStage对应的阻塞队列
        taskScheduler.registerBlockingQueue(reduceStageId, new LinkedBlockingQueue());
        // 根据reduceTaskNum去分配
        for(int i = 0; i < reduceTaskNum; i++){
            // 获取shuffle块的数据
            ShuffleBlockId[] stageShuffleIds = taskScheduler.getStageShuffleIdByReduceId(mapStageId, i);
            // 对shuffle块进行聚合，然后继续给executor进行处理
            ReduceFunction<String, Integer, String, Integer> reduceFunction = new ReduceFunction<String, Integer, String, Integer>() {
                @Override
                public Stream<KeyValue<String, Integer>> reduce(Stream<KeyValue<String, Integer>> stream) {
                    HashMap<String, Integer> map = new HashMap<>();
                    TreeMap<KeyValue<String,Integer>,Integer> sortMap = new TreeMap<>(
                            new Comparator<KeyValue<String,Integer>>() {
                                @Override
                                public int compare(KeyValue<String,Integer> o1, KeyValue<String,Integer> o2) {
                                    if(o1.getValue() > o2.getValue()){
                                        return -1;
                                    }else if (o1.getValue() <= o2.getValue()){
                                        return 1;
                                    }
                                    return 0;
                                }
                            }
                    );
                    //todo 学生实现 定义reducetask处理数据的规则
                    stream.forEach(e->{
                        String key = e.getKey();
                        Integer value  = e.getValue();
                        if(map.containsKey(key)){
                            map.put(key,map.get(key)+value);
                        }else{
                            map.put(key,value);
                        }
                    });
                    // 排序
                    for(String key : map.keySet()){
                        sortMap.put(new KeyValue<String,Integer>(key, map.get(key)),1);
                    }
                    System.out.println("--------"+map);
                    System.out.println("++++++++"+sortMap);
                    return sortMap.entrySet().stream().map(e -> new KeyValue(e.getKey().getKey(), e.getKey().getValue()));
                }
            };
            PartionWriter partionWriter = fileFormat.createWriter(outputPath, i);
            ReduceTaskContext reduceTaskContext = new ReduceTaskContext(applicationId,
                    "stage_" + reduceStageId,
                    taskScheduler.generateTaskId(),
                    i,
                    stageShuffleIds,// shuffle块
                    reduceFunction,// 聚合方法
                    partionWriter);//写结果的方法
            taskScheduler.addTaskContext(reduceStageId, reduceTaskContext);
        }

        DriverEnv.taskScheduler.submitTask(reduceStageId);
        DriverEnv.taskScheduler.waitStageFinish(reduceStageId);
        System.out.println("job finished");

    }
}
