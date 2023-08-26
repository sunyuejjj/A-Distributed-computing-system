package com.ksc.wordcount.task.map;

import com.ksc.wordcount.datasourceapi.PartionFile;
import com.ksc.wordcount.datasourceapi.PartionReader;
import com.ksc.wordcount.conf.AppConfig;
import com.ksc.wordcount.shuffle.DirectShuffleWriter;
import com.ksc.wordcount.task.Task;
import com.ksc.wordcount.task.map.MapFunction;
import com.ksc.wordcount.task.map.MapStatus;
import com.ksc.wordcount.task.map.MapTaskContext;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

public class ShuffleMapTask extends Task<MapStatus> {

    PartionFile partiongFile;
    PartionReader partionReader;
    int reduceTaskNum;
    MapFunction mapFunction;

    public ShuffleMapTask(MapTaskContext mapTaskContext) {
        super(mapTaskContext);//调用父类的构造方法
        this.partiongFile = mapTaskContext.getPartiongFile();
        this.partionReader = mapTaskContext.getPartionReader();
        this.reduceTaskNum = mapTaskContext.getReduceTaskNum();
        this.mapFunction = mapTaskContext.getMapFunction();
    }

    // 执行任务
    public MapStatus runTask() throws IOException {
        System.out.println("开始跑子任务了哦");
        // 从块中读取数据
        Stream<String> stream = partionReader.toStream(partiongFile);


//        Stream<AbstractMap.SimpleEntry<String, Integer>> simpleEntryStream = stream.flatMap(line -> Arrays.stream(line.split("\\s+")))
//                .map(word -> new AbstractMap.SimpleEntry<String, Integer>(word, 1));
        // 使用给定的方法去处理块中读到的数据，这里得到的是map的结果
        Stream kvStream = mapFunction.map(stream);
        // 为shuffle定义一个uuid
        String shuffleId= UUID.randomUUID().toString();
        //将task执行结果写入shuffle文件中
        DirectShuffleWriter shuffleWriter = new DirectShuffleWriter(AppConfig.shuffleTempDir,
                shuffleId,
                applicationId, // 自定义传入
                partionId, // 块id
                reduceTaskNum); // reduce任务的数量
        // 调用write进行shuffle写入
        shuffleWriter.write(kvStream); // 将map后的数据写入shuffle
        // 关闭写入流
        shuffleWriter.commit();
        return shuffleWriter.getMapStatus(taskId);
    }




}
