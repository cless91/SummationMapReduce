package org.swag.nummerical_summarization;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import mrdp.utils.MRDPUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * input key : offset in file, or input split
 * input value : the text comment
 * 
 *  output key : the id of the user --> raw xml attribute : string
 *  output value : min date, max date, nb comments --> custom data type --> 'MinMaxCountTuple'
 * @author cloudera
 *
 */
public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

	private Text mapOutputKey = new Text();
	private MinMaxCountTuple mapOutputValue = new MinMaxCountTuple();

	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Override
	protected void map(Object key, Text value, 
			Context context) throws IOException, InterruptedException {

		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		String strDate = parsed.get("creationDate");
		String userId = parsed.get("userId");

		if(strDate == null || userId == null){
			return;
		}

		Date creationDate;

		try {
			creationDate = frmt.parse(strDate);
			mapOutputValue.setMin(creationDate);
			mapOutputValue.setMax(creationDate);
			mapOutputValue.setCount(1);
			mapOutputKey.set(userId);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		context.write(mapOutputKey, mapOutputValue);
	
	}

}
