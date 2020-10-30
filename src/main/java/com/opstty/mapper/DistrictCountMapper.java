package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String str = value.toString();
        for(int s = 0; s < str.split(";").length; s++){
            if(s == 1 && !str.split(";")[0].contains("GEOPOINT")) {
                context.write(new Text(str.split(";")[s]), one);
            }

        }
    }
}
