import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Stripes {

	public static class StripeMapper
    extends Mapper<LongWritable, Text, Text, myMapWritable>{

		private myMapWritable neighborMap = new myMapWritable(); 
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] splittedWords = value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
			for(int i=0; i < splittedWords.length; i++){
				word.set(splittedWords[i]);
				neighborMap.clear();
				
				for(int j=i+1; j < splittedWords.length; j++){
					Text neighborWord = new Text(splittedWords[j]);
					if(neighborMap.containsKey(neighborWord)){
						IntWritable wordCnt = (IntWritable) neighborMap.get(neighborWord);
						wordCnt.set(wordCnt.get() + 1);
					}
					else{
						IntWritable const1 = new IntWritable(1);
						neighborMap.put(neighborWord, const1);
					}
				}
				
				context.write(word,neighborMap);
			}
		}
	}

	public static class StripeReducer
    	extends Reducer<Text,myMapWritable,Text,String> {
			private myMapWritable resultMap = new myMapWritable();

			public void reduce(Text key, Iterable<myMapWritable> values,
					Context context
                    	) throws IOException, InterruptedException {
				resultMap.clear();
				for(myMapWritable eachMap : values){
					Set<Writable> keys = eachMap.keySet();
					for(Writable eachKey : keys){
						IntWritable finalCnt = (IntWritable)eachMap.get(eachKey);
						if(resultMap.containsKey(eachKey)){
							IntWritable tempCnt = (IntWritable)resultMap.get(eachKey);
							finalCnt.set(finalCnt.get() + tempCnt.get());
							resultMap.put(eachKey, finalCnt);
						}
						else{
							resultMap.put(eachKey, finalCnt);
						}
						
					}
				}
				
				context.write(key, resultMap.toString());
			}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CoOccurence");
		job.setJarByClass(CoOcc.class);
		job.setMapperClass(StripeMapper.class);
		job.setReducerClass(StripeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(myMapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(String.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
