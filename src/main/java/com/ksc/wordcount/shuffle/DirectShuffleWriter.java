package com.ksc.wordcount.shuffle;

import com.ksc.wordcount.task.KeyValue;
import com.ksc.wordcount.task.map.MapStatus;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DirectShuffleWriter implements ShuffleWriter<KeyValue> {

    String baseDir;

    int reduceTaskNum;

    ObjectOutputStream[] fileWriters;

    ShuffleBlockId[] shuffleBlockIds ;

    public DirectShuffleWriter(String baseDir,String shuffleId,String  applicationId,int mapId, int reduceTaskNum) {
        this.baseDir = baseDir;
        this.reduceTaskNum = reduceTaskNum;
        fileWriters = new ObjectOutputStream[reduceTaskNum];
        shuffleBlockIds = new ShuffleBlockId[reduceTaskNum];
        // 对于每个reduce任务，mapId是块id
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                shuffleBlockIds[i]=new ShuffleBlockId(baseDir,applicationId,shuffleId,mapId,i);
                System.out.println(shuffleBlockIds[i].getShufflePath());
                new File(shuffleBlockIds[i].getShuffleParentPath()).mkdirs(); // 根据shuffleId创建目录
                // 创建一个写入流
                fileWriters[i] = new ObjectOutputStream(new FileOutputStream(shuffleBlockIds[i].getShufflePath()));

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //todo 学生实现 将maptask的处理结果写入shuffle文件中
    @Override
    public void write(Stream<KeyValue> entryStream) throws IOException {
        List<KeyValue> stringList = new ArrayList<>();
        Iterator<KeyValue> iterator = entryStream.iterator();
        while(iterator.hasNext()){
            KeyValue next = iterator.next();// KeyValue -> url : 1，尽量将相同key写到一个reduce
            stringList.add(next);
            fileWriters[next.getKey().hashCode()%reduceTaskNum].writeObject(next); // 根据hash进行obj的写入，本质依然是向文件写入map后的结果
        }
//        System.out.println(baseDir);
//        try {
//            // 创建一个 FileWriter 对象，并指定文件路径
//            FileWriter fileWriter = new FileWriter(baseDir);
//
//            // 写入文本内容
//            fileWriter.write("Line 1\n");
//            fileWriter.write("Line 2\n");
//            fileWriter.write("Line 3\n");
//
//            // 关闭 FileWriter
//            fileWriter.close();
//
//            System.out.println("File write completed.");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void commit() {
        for (int i = 0; i < reduceTaskNum; i++) {
            try {
                fileWriters[i].close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public  MapStatus getMapStatus(int mapTaskId) {
        return new MapStatus(mapTaskId,shuffleBlockIds); // 存放的是任务id和对于的shuffle结果的队列，队列里放的实际上是shffle块的信息而不是数据
    }


}
