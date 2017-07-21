import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class lemmaCount {
	
	static MapWritable hashMap = new MapWritable();

	public static class lemmaMapper
    extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			if(!(value.toString().isEmpty())){
				
				String[] splittedWords = value.toString().split(">");
				
				//get the payload part in correct format
				String payload = new String();
				String[] spltPayload = splittedWords[0].split("\\s+");
				if(spltPayload.length > 2){
					String lineNum  = spltPayload[2];
					String[] spltLineNum = lineNum.split("\\.");
					payload = spltPayload[0] + spltPayload[1] + "," + "[" + spltLineNum[0] + "," + spltLineNum[1] + "]" + ">";
				}
				else{
					String lineNum  = spltPayload[1];
					String[] spltLineNum = lineNum.split("\\.");
					payload = spltPayload[0] + "," + "[" + spltLineNum[0] + "," + spltLineNum[1] + "]" + ">";
				}
				
				// get the words from string
				String[] spltwords = splittedWords[1].replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
				for(int i = 0; i < spltwords.length; i++){
					
					//i,j,u,v transformation
					int indJ = spltwords[i].indexOf("j");
					int indV = spltwords[i].indexOf("v");
					
					String curWord = spltwords[i];
					if(indJ >= 0){
						curWord = curWord.replace("j", "i");
					}
					
					if(indV >= 0){
						curWord = curWord.replace("v", "u");
					}
					
					//combine currentword and payload
					String valOutMap = curWord + "," + payload;
					Text valOutMapT = new Text();
					valOutMapT.set(valOutMap);
					
					// check in hashmap
					Text curWordText = new Text();
					curWordText.set(curWord);
					if(hashMap.containsKey(curWordText)){
						Text val = (Text) hashMap.get(curWordText);
						String valStr = val.toString();
						String[] valArray = valStr.split(",");
						Text valOut = new Text();
						for(int m = 0; m < valArray.length; m++){
							valOut.set(valArray[m]);
							context.write(valOut, valOutMapT);
						}
					}
					else{
						context.write(curWordText, valOutMapT);
					}
				}
				
			}
			
		}
			
	}
	
	public static class lemmaReducer
    extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			
			StringBuilder rOut = new StringBuilder();
			for(Text tempVal : values){
				rOut.append(tempVal.toString());
				rOut.append(",");
			}
			
			Text rOutText = new Text();
			rOutText.set(rOut.toString());
			context.write(key, rOutText);
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String fileInput = "/home/hadoop/new_lemmatizer.csv";
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(fileInput))));
		String line = br.readLine();
		while(line!=null){
				
			Text key = new Text();
			Text value = new Text();
			String[] lineSplitted = line.split(",");
			key.set(lineSplitted[0]);
			StringBuilder valueStr = new StringBuilder();

			for(int i = 1; i < lineSplitted.length; i++){
				if(!(lineSplitted[i].isEmpty())){
					valueStr.append(lineSplitted[i]);
					valueStr.append(",");
				}
			}
			value.set(valueStr.toString());
			hashMap.put(key,value);
			line = br.readLine();
		} 
		Job job = Job.getInstance(conf, "lemma");
		job.setJarByClass(lemmaCount.class);
		job.setMapperClass(lemmaMapper.class);
		job.setReducerClass(lemmaReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
