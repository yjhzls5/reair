package com.airbnb.di.hive.batchreplication;

import com.airbnb.di.hive.batchreplication.hdfscopy.DirScanInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

import java.util.List;


public class FileSystemTestJob {
    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        DirScanInputFormat inputFormat = new DirScanInputFormat();
        config.set("mapred.input.dir", args[0]);

        Job job = new Job(config, "dummyjob");

        List<InputSplit> splits = inputFormat.getSplits(job);

        for (InputSplit p : splits) {
            System.out.println(p.getLocations());
            System.out.println(p.getLength());
        }
    }
}
