package com.opstty.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HeightBySpeciesReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        float max = Integer.MIN_VALUE;
        for (Text val : values) {
            if (val.toString() != "") {
                float flt = Float.parseFloat(val.toString());
                if (flt > max) {
                    max = flt;
                }
            }
        }
        context.write(key, new Text(Float.toString(max)));
    }
}
