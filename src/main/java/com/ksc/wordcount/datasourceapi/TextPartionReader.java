package com.ksc.wordcount.datasourceapi;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class TextPartionReader implements PartionReader<String>, Serializable {

    private Stream<String>  getData(FileSplit fileSplit){
        Stream<String> allStream  = Stream.empty();
        try(BufferedReader reader = new BufferedReader(new FileReader(fileSplit.getFileName()))){
            String line;
            int start = 0;
            int length = 0;
            while((line = reader.readLine())!=null){
                int lineSize = line.getBytes().length;
                start += lineSize;
                if(start > fileSplit.getStart()){
                    // 开始读取数据
                    allStream = Stream.concat(allStream,Stream.of(line));
                    length += lineSize;
//                    System.out.println("getData 读到的数据是"+line);
                    if(length>= fileSplit.getLength()){
                        break;
                    }
                }
            }
            return allStream;
        }catch (IOException e){
            return allStream;
        }
    }
    @Override
    public Stream<String> toStream(PartionFile partionFile) throws IOException {
        Stream<String> allStream = Stream.empty();
        //todo 学生实现 maptask读取原始数据文件的内容
        // 从块中取出所有文件进行读取，讲道理应该更加块所标记的位置进行读取
        System.out.println("TextPartionReader.java ---  Stream<String> toStream(PartionFile partionFile)");
        for(FileSplit fileSplit : partionFile.getFileSplits()){
//            Stream<String> lineStream = Files.lines(Paths.get(fileSplit.getFileName()));
            Stream<String> lineStream = getData(fileSplit);
            allStream = Stream.concat(allStream,lineStream);
        }
        return allStream;
    }
}
