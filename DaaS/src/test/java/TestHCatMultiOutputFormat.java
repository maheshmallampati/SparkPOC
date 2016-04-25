import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.cli.HCatCli;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;

import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hcatalog.mapreduce.MultiOutputFormat;
import org.apache.hcatalog.mapreduce.MultiOutputFormat.JobConfigurer;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

/**
 * 
 * @author Sateesh Pula
 *export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/psinkula/scripts/daasdependencyjarfiles/sqljdbc4.jar:
  /home/psinkula/scripts/daasdependencyjarfiles/tdgssconfig.jar:/home/psinkula/scripts/daasdependencyjarfiles/terajdbc4.jar:
  /usr/lib/hive/lib/hive-exec-0.11.0.1.3.3.0-58.jar:/usr/lib/hcatalog/share/hcatalog/hcatalog-core.jar:
  /usr/lib/hive/lib/hive-metastore-0.11.0.1.3.3.0-58.jar:/usr/lib/hive/lib/libfb303-0.9.0.jar:
  /usr/lib/hive/lib/jdo2-api-2.3-ec.jar:/usr/lib/hive/lib/libthrift-0.9.0.jar:/usr/lib/hive/lib/antlr-runtime-3.4.jar:
  /usr/lib/hive/lib/datanucleus-api-jdo-3.0.7.jar:/usr/lib/hive/lib/datanucleus-core-3.0.9.jar


   export LIBJARS=/home/psinkula/scripts/daasdependencyjarfiles/sqljdbc4.jar,
   /home/psinkula/scripts/daasdependencyjarfiles/tdgssconfig.jar,/home/psinkula/scripts/daasdependencyjarfiles/terajdbc4.jar,
   /usr/lib/hive/lib/hive-exec-0.11.0.1.3.3.0-58.jar,/usr/lib/hcatalog/share/hcatalog/hcatalog-core.jar,
   /usr/lib/hive/lib/hive-metastore-0.11.0.1.3.3.0-58.jar,/usr/lib/hive/lib/libfb303-0.9.0.jar,
   /usr/lib/hive/lib/jdo2-api-2.3-ec.jar,/usr/lib/hive/lib/libthrift-0.9.0.jar,/usr/lib/hive/lib/antlr-runtime-3.4.jar,
    /usr/lib/hive/lib/datanucleus-api-jdo-3.0.7.jar,/usr/lib/hive/lib/datanucleus-core-3.0.9.jar

hadoop jar /home/psinkula/scripts/daasmapreduce.jar TestHCatMultiOutputFormat -libjars $LIBJARS \
-Dhive.metastore.uris=thrift://hdp001-nn:9083 \
/daastest/tdapmix/1000,/daastest/tdasales/tdasales1000 \

 */
public class TestHCatMultiOutputFormat extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		   int exitCode = ToolRunner.run(new TestHCatMultiOutputFormat(), args);
	        System.exit(exitCode);
	}
	
	static String[] tableNames = {"tdapmix_1000_orc","tdasales_1000_orc"};
	private static class MyMapper extends Mapper<LongWritable, Text, BytesWritable, HCatRecord> {


		private int i = 0;
		String fileName ="sales";
		
		
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			FileSplit fs = ((FileSplit)context.getInputSplit());
			fileName = fs.getPath().toString();
		}



		@Override
		protected void map(LongWritable key, Text value, Context context)
		        throws IOException, InterruptedException {
		    HCatRecord record = null;
		    
		    
		    String[] values = value.toString().split("\\|");
		    
		    if(fileName.toUpperCase().contains("PMIX")){
		    	
		    	record = new DefaultHCatRecord(11);
	        	
	        	
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
	        	
	        	MultiOutputFormat.write(tableNames[0], null, record, context);
	        	
		    }else{
		    	record = new DefaultHCatRecord(17);
	        	
	        	
	        	record.set(0, values[0]);
	        	record.set(1, Integer.parseInt(values[1]));
	        	record.set(2, Integer.parseInt(values[2]));
	        	record.set(3, values[3]);
	        	record.set(4, values[4]);
	        	record.set(5, values[5]);
	        	record.set(6, values[6]);
	        	record.set(7, values[7]);
	        	record.set(8, values[8]);
	        	record.set(9, Double.parseDouble(values[9]));
	        	record.set(10, Double.parseDouble(values[10]));
	        	record.set(11, Double.parseDouble(values[11]));
	        	record.set(12, Integer.parseInt(values[12]));
	        	record.set(13, Integer.parseInt(values[13]));
	        	record.set(14, values[14]);
	        	record.set(15, values[15]);
	        	record.set(16, Integer.parseInt(values[16]));
	        	
	        	MultiOutputFormat.write(tableNames[1], null, record, context);
		    }
		    
		    
		    
		    
		}
}

	
	 public int run(String[] args) throws Exception {
			
			
			List<HCatFieldSchema> pmixFldSchema = new ArrayList<HCatFieldSchema>();
			 
			pmixFldSchema.add(new HCatFieldSchema("lgcy_lcl_rfr_def_cd",Type.STRING,""));
			pmixFldSchema.add(new HCatFieldSchema("reg_id",Type.INT,""));
			pmixFldSchema.add(new HCatFieldSchema("pos_busn_dt",Type.STRING,""));
			pmixFldSchema.add(new HCatFieldSchema("ord_num",Type.INT,""));
			pmixFldSchema.add(new HCatFieldSchema("menu_itm_id",Type.INT,""));
			pmixFldSchema.add(new HCatFieldSchema("item_price",Type.DOUBLE,""));
			pmixFldSchema.add(new HCatFieldSchema("item_qty",Type.DOUBLE,""));
			pmixFldSchema.add(new HCatFieldSchema("item_qty_promo",Type.DOUBLE,""));
			pmixFldSchema.add(new HCatFieldSchema("item_tax_rate",Type.DOUBLE,""));
			pmixFldSchema.add(new HCatFieldSchema("vat_rounding",Type.INT,""));
			pmixFldSchema.add(new HCatFieldSchema("ctry_iso_nu",Type.INT,""));
			
			
			List<HCatFieldSchema> salesFldSchema = new ArrayList<HCatFieldSchema>();
			 
			salesFldSchema.add(new HCatFieldSchema("lgcy_lcl_rfr_def_cd",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("reg_id",Type.INT,""));
			
			salesFldSchema.add(new HCatFieldSchema("ord_num",Type.INT,""));
			salesFldSchema.add(new HCatFieldSchema("pos_busn_dt",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("ord_dt",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("ord_tm",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("pod",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("sale_type",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("ord_kind",Type.STRING,""));
			
			salesFldSchema.add(new HCatFieldSchema("bd_tot_am",Type.DOUBLE,""));
			salesFldSchema.add(new HCatFieldSchema("bp_tot_am",Type.DOUBLE,""));
			salesFldSchema.add(new HCatFieldSchema("ord_tot_am",Type.DOUBLE,""));
			
			salesFldSchema.add(new HCatFieldSchema("held_time",Type.INT,""));
			salesFldSchema.add(new HCatFieldSchema("total_time",Type.INT,""));
			salesFldSchema.add(new HCatFieldSchema("menupricebasis",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("tender_name",Type.STRING,""));
			salesFldSchema.add(new HCatFieldSchema("ctry_iso_nu",Type.INT,""));
			
			HashMap<String, HCatSchema> schemaMap = new HashMap<String, HCatSchema>();
			
			HCatSchema pmixHcatSchema = new HCatSchema(pmixFldSchema);
			HCatSchema salesHcatSchema = new HCatSchema(salesFldSchema);
			
			schemaMap.put(tableNames[0], pmixHcatSchema);
			schemaMap.put(tableNames[1], salesHcatSchema);
			
			HashMap<String,String> partitions = new HashMap<String,String>();
			partitions.put("dt", "20140930");
			
			
			HiveMetaStoreClient hiveCli = new HiveMetaStoreClient(new HiveConf(getConf(),TestHCatMultiOutputFormat.class));
			List<String> partitionVals = new ArrayList<String>();
			partitionVals.add("dt");
//			partitionVals.add("20140931");
			
			Hive hv = Hive.get(new HiveConf(getConf(),TestHCatMultiOutputFormat.class));
			
			
			
//			Table pmixTbl = hiveCli.getTable("default", tableNames[1]);
//			
//			List<FieldSchema> partKeys = pmixTbl.getPartitionKeys();
//			
			
			try{
//				Partition part = hiveCli.getPartition("default", tableNames[0], "dt=20140930") ;
//				hiveCli.dropPartitionByName("default",tableNames[0], "dt=20140930/", true);
			
				if(hiveCli.getPartition("default", tableNames[1], "dt=20140930") != null){
				
					if(hiveCli.dropPartitionByName("default",tableNames[1], "dt=20140930", true)){
						System.out.println(" partition deleted successfully ****");
					}else{
						System.out.println(" partition could not be deleted ****");
					}
				
				}else{
					System.out.println(" partition does not exist");
				}
				
//				hv.dropPartition("default", tableNames[1], partitionVals, true);
				
			}catch(NoSuchObjectException nsoex){
				System.out.println(" skipping partition delete ");
			}
			catch(Exception ex){
				ex.printStackTrace();
				System.out.println(" partitions does not exist");
			}
			 
		        ArrayList<OutputJobInfo> infoList = new ArrayList<OutputJobInfo>();
		        infoList.add(OutputJobInfo.create("default", tableNames[0], partitions));
		        infoList.add(OutputJobInfo.create("default", tableNames[1], partitions));


		        Job job = new Job(getConf(), "SampleJob");

		        job.setMapperClass(MyMapper.class);
		        job.setJarByClass(TestHCatMultiOutputFormat.class);
		        
		        job.setInputFormatClass(TextInputFormat.class);
		        job.setOutputFormatClass(MultiOutputFormat.class);
		        job.setNumReduceTasks(0);

		        JobConfigurer configurer = MultiOutputFormat.createConfigurer(job);

		        for (int i = 0; i < tableNames.length; i++) {
		            configurer.addOutputFormat(tableNames[i], HCatOutputFormat.class, BytesWritable.class,
		                    HCatRecord.class);
		            HCatOutputFormat.setOutput(configurer.getJob(tableNames[i]), infoList.get(i));
		            HCatOutputFormat.setSchema(configurer.getJob(tableNames[i]),schemaMap.get(tableNames[i]));
		        }
		        configurer.configure();

		       
		        FileInputFormat.setInputPaths(job, args[0]);
		       
//			job.waitForCompletion(true);
		           
			return 0;
		}
}
