/**
 * 
 */
package com.mcd.gdw.daas.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @author mc41946
 *
 */
public class GetFileFromHDFS extends Configured implements Tool{
	

	
	/*getFile Result: Will give the .gz file location  for the store as key and value as xml in text format

	Args: inputpath, output Path, storied


	FindFile: will give list of storeid as key and its file path as value
	Args: inputpath,outputpath*/

   //hadoop jar ${EXE_PATH}/daasmapreduceyarn.jar com.mcd.gdw.daas.driver.GetFileFromHDFS /daas/gold/np_xml/STLD/840/20150405/* /daastest/goldfiles 33178
    public static class FindFileMapper extends
    Mapper<LongWritable, Text, Text, Text> {
		
		String filename = ""; 
		Context thiscontext;
		String input_val;
		public void setup(Context context) throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
			 input_val=context.getConfiguration().get("storeid");
			  //fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			// thiscontext = context.getConfiguration();
				//super.setup(context);
			}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] recArr = value.toString().split("\t"); 
			String storid=recArr[5];
		
			//System.out.println("Storeid"+input_val);
			input_val=context.getConfiguration().get("storeid");
			//System.out.println("Storeid"+input_val);
			if(!storid.equals(input_val))
			{
				return;
			}
			boolean writetofile = false;
			String nam ="";
		
				Path filePath = ((FileSplit) context.getInputSplit()).getPath();
				nam = ((FileSplit) context.getInputSplit()).getPath().toString();
				
				filename = filename +","+nam;
				writetofile=true;
					
			if(writetofile)
			{
			//context.write(new Text(nam+"::::::::::::"),value);
			context.write(new Text(nam+"::::::::::::"),value);
			}
			//System.exit(1);
		
	
        }
        
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            
            
            
        }
    
    }
    
    
    private void printUsage() {
        System.out.println("Usage : StldXmlParser <input_dir> <output>");
    }

    public int run(String[] args) throws Exception {
    	
    	if (args.length < 2) {
			printUsage();
			return 2;
		}
		
		//Job job = new Job(conf,"TLDXmlParser");

		
		 Configuration conf = new Configuration();
		
/*		System.out.println("Arguments of 2"+args[0]);
		System.out.println("Arguments of 2"+args[1]);
		System.out.println("Arguments of 2"+args[2]);*/
		//System.out.println(conf.get("input.storeid"));
		if (args.length >=2 ) {
			conf.set("storeid", args[2]);
		
		}
		//conf.setClass("mapred.input.compression.codec", GzipCodec.class, CompressionCodec.class);
		Job job = new Job(conf, "Generate TDA Format STLD Header/Detail");

		
		//job.setJobName("getFile");
		job.setJarByClass(GetFileFromHDFS.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FindFileMapper.class);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		Path outPath = new Path(args[1]);
		outPath.getFileSystem(conf).delete(outPath, true);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		String input_val=conf.get("storeid");
		System.out.println("Storeid"+input_val);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new GetFileFromHDFS(), args);        

        System.exit(ret);
    }
    






	

}
