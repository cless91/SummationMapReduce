package org.swag.nummerical_summarization;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
	private MinMaxCountTuple result = new MinMaxCountTuple();

	@Override
	public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException{
		// TODO Auto-generated method stub
		result.setMin(null);
		result.setMax(null);
		int sum = 0;
		
		for (MinMaxCountTuple val : values) {
			
			if(result.getMin() == null || result.getMin().compareTo(val.getMin()) > 0){
				result.setMin(val.getMin());
			}
			
			if(result.getMax() == null || result.getMax().compareTo(val.getMax()) < 0){
				result.setMax(val.getMax());
			}
			
			sum += result.getCount();
		}
		
		result.setCount(sum);
		context.write(key, result);
	
		
	}

}
