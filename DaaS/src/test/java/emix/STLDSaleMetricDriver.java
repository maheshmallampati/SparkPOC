package emix;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.*;

@SuppressWarnings("deprecation")
public class STLDSaleMetricDriver extends Configured implements Tool {

	private void printUsage() {
		System.out.println("Usage : StldXmlParser <input_dir> <output> <Currency.psv, GiftItem.psv, primarytosecondarytest.psv, comboitemstest.psv, itemPrice.psv, GenericDrink.psv>");
	}

	// Configuration
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			printUsage();
			return 2;
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "STLDSaleMetrics");
		job.setJobName("STLDSaleMetrics");
		job.setJarByClass(STLDSaleMetricDriver.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(STLDSaleMetricMapper.class);
		job.setReducerClass(STLDSaleMetricReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outPath = new Path(args[1]);
		outPath.getFileSystem(conf).delete(outPath, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		DistributedCache.addCacheFile(new Path(args[2]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[3]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[4]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[5]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[6]).toUri(),
				job.getConfiguration());
		DistributedCache.addCacheFile(new Path(args[7]).toUri(),
				job.getConfiguration());

		MultipleOutputs.addNamedOutput(job, STLDConstants.DAILY, TextOutputFormat.class,
				NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, STLDConstants.HOURLY, TextOutputFormat.class,
				NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, STLDConstants.PMIX, TextOutputFormat.class,
				NullWritable.class, Text.class);

		MultipleOutputs.addNamedOutput(job, STLDConstants.Errors, TextOutputFormat.class,
				NullWritable.class, Text.class);

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		Date current_timeStamp = new Date();
		int job_status = job.waitForCompletion(true) ? 0 : 1;
		if (job_status == 0) {
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

			String timeStamp = format.format(current_timeStamp);
			FileSystem hdfs = FileSystem.get(conf);
			FileStatus fs[] = hdfs.listStatus(new Path(args[1]));

			for (int fileCounter = 0; fileCounter < fs.length; fileCounter++) {
				if (fs[fileCounter].getPath().getName().startsWith("Hourly")) {
					hdfs.rename(fs[fileCounter].getPath(), new Path(args[1]
							+ "/GLMA_Dy_Tm_Seg_Sls." + timeStamp + ".psv"));
				} else if (fs[fileCounter].getPath().getName().startsWith("Daily")) {
					hdfs.rename(fs[fileCounter].getPath(), new Path(args[1]
							+ "/GLMA_Dly_Sls." + timeStamp + ".psv"));
				} else if (fs[fileCounter].getPath().getName().startsWith("_")) {
					hdfs.delete(fs[fileCounter].getPath(), true);
				}else if (fs[fileCounter].getPath().getName().startsWith("PMix")) {
					hdfs.rename(fs[fileCounter].getPath(), new Path(args[1]
							+ "/GLMA_Dly_Pmix." + timeStamp + ".psv"));
				}
			}
		}
		return job_status;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new STLDSaleMetricDriver(), args);
		System.exit(status);
	}

}