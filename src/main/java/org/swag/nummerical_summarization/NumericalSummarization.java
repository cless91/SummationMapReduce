package org.swag.nummerical_summarization;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hello world!
 *
 */
public class NumericalSummarization 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        
        if(otherArgs.length != 2){
        	System.err.println("Usage : MinMaxCountDriver <in> <out>");
        	System.exit(2);
        }
        
        Job job = Job.getInstance(configuration, "Stackoverflow comments min max date count");
        job.setJarByClass(NumericalSummarization.class);
        
        job.setMapperClass(MinMaxCountMapper.class);
        job.setReducerClass(MinMaxCountReducer.class);
        job.setCombinerClass(MinMaxCountReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
