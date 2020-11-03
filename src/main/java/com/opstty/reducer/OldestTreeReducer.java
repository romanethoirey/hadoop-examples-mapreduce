package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int oldest_year = Integer.MAX_VALUE;
    private Text district = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        for(IntWritable value: values){
            if(value.get() < oldest_year){
                oldest_year = value.get();
                district.set(key);
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(district, new IntWritable(oldest_year));
    }
}
