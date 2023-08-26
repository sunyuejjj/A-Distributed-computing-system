package com.ksc.wordcount.datasourceapi;

import java.io.Serializable;

/**
 * 文件切分之后的对象
 */
public class PartionFile implements Serializable {

    private int partionId;
    private FileSplit[] fileSplits;

    public PartionFile(int partionId, FileSplit[] fileSplits) {
        this.partionId = partionId;
        this.fileSplits = fileSplits;
    }

    public int getPartionId() {
        return partionId;
    }

    public FileSplit[] getFileSplits() {
        return fileSplits;
    }

}
