package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AllSpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private NullWritable nullWritable = NullWritable.get();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String str = value.toString();
        String[] split = str.split(";");

        for(int index = 0; index < split.length; index++) {
            if(index == 3 && !split[0].contains("GEOPOINT")) {
                context.write(new Text(split[index]), nullWritable);
            }

        }
    }
}
