package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AllSpeciesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String str = value.toString();
        String[] split = str.split(";");

        for(int index = 0; index < split.length; index++) {
            if(index == 3 && !split[0].contains("GEOPOINT")) {
                context.write(new Text(split[index]), one);
            }

        }
    }
}
