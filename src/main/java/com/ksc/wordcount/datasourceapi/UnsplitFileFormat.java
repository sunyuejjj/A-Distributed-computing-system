package com.ksc.wordcount.datasourceapi;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class UnsplitFileFormat implements FileFormat {

        @Override
        public boolean isSplitable(String filePath) {
            return false;
        }

    /**
     * 文件分块，并保证不会将某行数据分到两个块中，因此块大小可能会超过size
     */
        private FileSplit[] splitFIle(File file,long size){
            List<FileSplit> fileSplits = new LinkedList<>();
            try(BufferedReader reader = new BufferedReader(new FileReader(file))){
                String line;
                int curSize = 0;
                int start = 0;
                int total = 0;
                while((line = reader.readLine())!=null){
                    int lineSize = line.getBytes().length;
                    curSize += lineSize;
                    total += lineSize;
                    if(curSize >= size){
                        // 创建新的块
                        fileSplits.add(new FileSplit(file.getAbsolutePath(),start,curSize));
                        start = total+1;
                        curSize = 0;
                    }
                }
                // 最后剩余的这部分也要存进去
                // 创建新的块
                fileSplits.add(new FileSplit(file.getAbsolutePath(),start,curSize));
                return fileSplits.toArray(new FileSplit[0]);//若给的参数小于实际的长度，LinkedList会根据实际的长度去创建
            }catch (IOException e){
                return new FileSplit[0];
            }
        }

        @Override
        public PartionFile[] getSplits(String filePath, long size) {
            // todo 学生实现 driver端切分split的逻辑，应该按size切分，同时保证同一行文件不会被切成两部分
            // 如果是文件
            File parentFile = new File(filePath);
            if(parentFile.isFile()){
                return new PartionFile[]{new PartionFile(0,splitFIle(parentFile,size))};
//                return new PartionFile[]{new PartionFile(0,new FileSplit[]{new FileSplit(filePath,0,parentFile.length())})};
            }
            // 如果是文件夹
            List<PartionFile> partiongFileList=new ArrayList<>();
            int partId = 0;
            File[] files = parentFile.listFiles();
            for(File file : files){
                //文件切分为块
//                FileSplit[] splits = {new FileSplit(file.getAbsolutePath(),0,file.length())};
                FileSplit[] splits = splitFIle(file,size);
                        // 文件块存入块类
                PartionFile partionFile = new PartionFile(partId,splits);
                partiongFileList.add(partionFile);
                partId++;
            }
            return partiongFileList.toArray(new PartionFile[partiongFileList.size()]);
        }

    @Override
    public PartionReader createReader() {
        return new TextPartionReader();
    }

    @Override
    public PartionWriter createWriter(String destPath, int partionId) {
        return new TextPartionWriter(destPath, partionId);
    }


}
