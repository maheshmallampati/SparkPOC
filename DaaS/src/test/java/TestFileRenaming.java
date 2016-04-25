import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TestFileRenaming {

	public static void main(String[] args){
		
		  BufferedReader br = null;
		  InputStreamReader in = null;
		  FSDataInputStream fsIn = null;
		  try{
			
			Configuration conf = new Configuration();
			if(args[0].equalsIgnoreCase("local")){//this will not work. network will not allow.
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/core-site.xml"));
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/hdfs-site.xml"));
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/mapred-site.xml"));
			}else{
				conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
			}
			
			
			
			FileSystem hdfsFileSystem = FileSystem.get(conf);
			
			FileStatus[] fileStats = hdfsFileSystem.listStatus(new Path(args[1]));
			
			System.out.println("fileStats " +fileStats.length);
			
			HashMap<String,String> newMap = new HashMap<String,String>();
			
			
			
			
			
			if(fileStats != null ){
				String fileName;
				String[] fileNames;
				for(int i =0;i<fileStats.length;i++){
					
					fileName = fileStats[i].getPath().getName();
					
					fileNames = fileName.split("-");
					String line;
					if("SALES".equalsIgnoreCase(fileNames[0])){
						
						 fsIn = hdfsFileSystem.open(fileStats[i].getPath());
						 in = new InputStreamReader(fsIn);
						 br  = new BufferedReader(in); 
						 
						 line = br.readLine();
						 
						 System.out.println(" line " + line);
						 
						 System.out.println(" store id in " +fileName + " - " +line.split("\\|")[0]);
						 
						
					}
//					System.out.println(" fileStats  " + fileNames[1]);
					if(br != null){
						br.close();
						br = null;
					}
					
					if(in != null){
						in.close();
						in = null;
					}
					
					if(fsIn != null){
						fsIn.close();
						fsIn = null;
					}
				}
					
			}
			
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				if(br != null){
					br.close();
					br = null;
				}
				
				if(in != null){
					in.close();
					in = null;
				}
				
				if(fsIn != null){
					fsIn.close();
					fsIn = null;
				}
					
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text("Total Lines");
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
                context.write(word, one);
        }
}
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		Iterator<Text> itvalues = values.iterator();
		//for (Text value : values ) {
		
		
		while (itvalues.hasNext()) {
		//	value = itvalues.next();
			sum ++;
			itvalues.next();
		}
		context.write(key, new IntWritable(sum));
	}
}

	public void lineCount(Configuration conf) throws Exception {
		Job job = new Job(conf, "LineCount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(""));
		FileOutputFormat.setOutputPath(job, new Path(""));
		job.waitForCompletion(true);
	}
}
