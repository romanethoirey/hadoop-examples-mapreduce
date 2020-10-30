package com.opstty.job;

import com.opstty.mapper.AllSpeciesMapper;
import com.opstty.mapper.DistrictCountMapper;
import com.opstty.mapper.TokenizerMapper;
import com.opstty.reducer.AllSpeciesReducer;
import com.opstty.reducer.DistrictCountReducer;
import com.opstty.reducer.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Launcher {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        switch (otherArgs[0]){
            case "wordcount":

                if (otherArgs.length < 3) {
                    System.err.println("Usage: wordcount <in> [<in>...] <out>");
                    System.exit(2);
                }

                Job wordcount = Job.getInstance(conf, "wordcount");
                wordcount.setJarByClass(Launcher.class);
                wordcount.setMapperClass(TokenizerMapper.class);
                wordcount.setCombinerClass(IntSumReducer.class);
                wordcount.setReducerClass(IntSumReducer.class);
                wordcount.setOutputKeyClass(Text.class);
                wordcount.setOutputValueClass(IntWritable.class);
                for (int i = 0; i < otherArgs.length - 1; ++i) {
                    FileInputFormat.addInputPath(wordcount, new Path(otherArgs[i]));
                }
                FileOutputFormat.setOutputPath(wordcount,
                        new Path(otherArgs[otherArgs.length - 1]));
                System.exit(wordcount.waitForCompletion(true) ? 0 : 1);
                break;

            case "district_containing_trees":
                if (otherArgs.length < 2) {
                    System.err.println("Usage: wordcount <in> [<in>...] <out>");
                    System.exit(2);
                }

                Job district_containing_trees = Job.getInstance(conf, "district_containing_trees");
                district_containing_trees.setJarByClass(Launcher.class);
                district_containing_trees.setMapperClass(DistrictCountMapper.class);
                district_containing_trees.setCombinerClass(DistrictCountReducer.class);
                district_containing_trees.setReducerClass(DistrictCountReducer.class);
                district_containing_trees.setOutputKeyClass(Text.class);
                district_containing_trees.setOutputValueClass(IntWritable.class);


                FileInputFormat.addInputPath(district_containing_trees, new Path(otherArgs[1]));
                FileOutputFormat.setOutputPath(district_containing_trees, new Path(otherArgs[2]));

                System.exit(district_containing_trees.waitForCompletion(true) ? 0 : 1);
                break;

            case "all_species":
                if (otherArgs.length < 2) {
                    System.err.println("Usage: all_species <in> [<in>...] <out>");
                    System.exit(2);
                }

                Job all_species = Job.getInstance(conf, "all_species");
                all_species.setJarByClass(Launcher.class);
                all_species.setMapperClass(AllSpeciesMapper.class);
                all_species.setCombinerClass(AllSpeciesReducer.class);
                all_species.setReducerClass(AllSpeciesReducer.class);
                all_species.setOutputKeyClass(Text.class);
                all_species.setOutputValueClass(IntWritable.class);


                FileInputFormat.addInputPath(all_species, new Path(otherArgs[1]));
                FileOutputFormat.setOutputPath(all_species, new Path(otherArgs[2]));

                System.exit(all_species.waitForCompletion(true) ? 0 : 1);
                break;

        }
    }
}