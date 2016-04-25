import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

/**
 * 
 * @author Sateesh Pula
 *
 * 
 */

/*
 * Script to run the program
 * 
 * export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/psinkula/scripts/daasdependencyjarfiles/sqljdbc4.jar:/home/psinkula/scripts/daasdependencyjarfiles/tdgssconfig.jar:
 * /home/psinkula/scripts/daasdependencyjarfiles/terajdbc4.jar:/usr/lib/hive/lib/hive-exec-0.11.0.1.3.3.0-58.jar:/usr/lib/hcatalog/share/hcatalog/hcatalog-core.jar:
 * /usr/lib/hive/lib/hive-metastore-0.11.0.1.3.3.0-58.jar:/usr/lib/hive/lib/libfb303-0.9.0.jar:/usr/lib/hive/lib/jdo2-api-2.3-ec.jar:/usr/lib/hive/lib/libthrift-0.9.0.jar:
 * /usr/lib/hive/lib/antlr-runtime-3.4.jar:/usr/lib/hive/lib/datanucleus-api-jdo-3.0.7.jar:/usr/lib/hive/lib/datanucleus-core-3.0.9.jar


   export LIBJARS=/home/psinkula/scripts/daasdependencyjarfiles/sqljdbc4.jar,/home/psinkula/scripts/daasdependencyjarfiles/tdgssconfig.jar,
   /home/psinkula/scripts/daasdependencyjarfiles/terajdbc4.jar,/usr/lib/hive/lib/hive-exec-0.11.0.1.3.3.0-58.jar,/usr/lib/hcatalog/share/hcatalog/hcatalog-core.jar,
   /usr/lib/hive/lib/hive-metastore-0.11.0.1.3.3.0-58.jar,/usr/lib/hive/lib/libfb303-0.9.0.jar,/usr/lib/hive/lib/jdo2-api-2.3-ec.jar,/usr/lib/hive/lib/libthrift-0.9.0.jar,
    /usr/lib/hive/lib/antlr-runtime-3.4.jar,/usr/lib/hive/lib/datanucleus-api-jdo-3.0.7.jar,/usr/lib/hive/lib/datanucleus-core-3.0.9.jar

	hadoop jar /home/psinkula/scripts/daasmapreduce.jar TestHCatOutputFormat -libjars $LIBJARS \
	-Dhive.metastore.uris=thrift://hdp001-nn:9083 \
	/daastest/tdapmix/1000 \
	tdapmix_1000_orc \

 */

public class TestHCatOutputFormat extends Configured implements Tool {

    public static class Map extends
            Mapper<LongWritable, Text, NullWritable, HCatRecord> {

     

        @Override
        protected void map(
                LongWritable key,
                Text value,
                org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,NullWritable, HCatRecord>.Context context)
                throws IOException, InterruptedException {
        	
        	String[] values = value.toString().split("\\|");
        	
        	
        	
        	
        	HCatRecord record = new DefaultHCatRecord(11);
        	
        	
        	record.set(0, values[0]);
        	record.set(1, Integer.parseInt(values[1]));
        	record.set(2, values[2]);
        	record.set(3, Integer.parseInt(values[3]));
        	record.set(4, Integer.parseInt(values[4]));
        	record.set(5, Double.parseDouble(values[5]));
        	record.set(6, Double.parseDouble(values[6]));
        	record.set(7, Double.parseDouble(values[7]));
        	record.set(8, Double.parseDouble(values[8]));
        	record.set(9, Integer.parseInt(values[9]));
        	record.set(10,Integer.parseInt(values[10]));
        	

            context.write(NullWritable.get(), record);

        }
    }


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        String inputPath	   = args[0];
        String outputTableName = args[1];
        

//        conf.set("hive.metastore.uris", "thrift://hdp001-nn:9083");
        
        Job job = new Job(conf, "TestHCat");
        
        // initialize HCatOutputFormat

        job.setInputFormatClass(TextInputFormat.class);
        
        job.setJarByClass(TestHCatOutputFormat.class);
        job.setMapperClass(Map.class);
        
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DefaultHCatRecord.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        
        
        
        
        HCatOutputFormat.setOutput(job, OutputJobInfo.create("default",outputTableName, null));
        HCatSchema s = HCatOutputFormat.getTableSchema(job);
        System.err.println("INFO: output schema explicitly set for writing:"
                + s);
        HCatOutputFormat.setSchema(job, s);
        job.setOutputFormatClass(HCatOutputFormat.class);
        
        job.setNumReduceTasks(0);
        
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TestHCatOutputFormat(), args);
        System.exit(exitCode);
    }
}