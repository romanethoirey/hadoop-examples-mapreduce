package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AllSpeciesReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    private NullWritable nullWritable = NullWritable.get();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, nullWritable);
    }
}
