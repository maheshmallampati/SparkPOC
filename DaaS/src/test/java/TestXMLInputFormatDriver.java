import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class TestXMLInputFormatDriver  extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		
		
		int retval = ToolRunner.run(new TestXMLInputFormatDriver(), args);
		
	}
	@Override
	public int run(String[] argsall) throws Exception {
		
		GenericOptionsParser gop = new GenericOptionsParser(argsall);
		String[] args = gop.getRemainingArgs();
		
		Configuration conf = super.getConf();
		
		conf.set("xmlinput.start","<Order");
		conf.set("xmlinput.end","</Order>");
		Job job = new Job(conf,"test");
		
//		job.setInputFormatClass(org.apache.mahout.text.wikipedia.XmlInputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(TestXMLInputFormatMapper.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(TestXMLInputFormatMapper.class);
		
		job.setNumReduceTasks(0);
		
		int retCode = job.waitForCompletion(true) ? 0 : 1;
		
		
		return retCode;
	}
	

}
