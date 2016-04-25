package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NextGenUnzipFileListReducer extends Reducer<Text, Text, NullWritable, Text> {

	private Text outValue = new Text();
	private Iterator<Text> valIt;
	private HashMap<String,String> uniqueList = new HashMap<String,String>();
	
	private MultipleOutputs<NullWritable, Text> mos;
	private String keyText;
	private int rowCnt;
	private int delCnt;
	private String[] parts;
	private Path tmpPath;
	private int max;
	private int curr;
	
	@Override
	public void setup(Context context) {

		mos = new MultipleOutputs<NullWritable, Text>(context);

	}
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		keyText = key.toString();
		valIt = values.iterator();
		
		if ( keyText.startsWith("REMOVE") ) {
			while ( valIt.hasNext() ) {
				outValue = valIt.next();
				parts = outValue.toString().split("\t");
				
				if ( !uniqueList.containsKey(parts[0]) ) {
					uniqueList.put(parts[0], "");
				}

				tmpPath = new Path(parts[0]);

				outValue.clear();
				outValue.set(tmpPath.getName() + "\t" + parts[1]);
				
				mos.write(NullWritable.get(), outValue, "removeitems");
			}
			
			for (Map.Entry<String,String> entry : uniqueList.entrySet()) {
				outValue.clear();
				outValue.set(entry.getKey().toString());
				
				mos.write(NullWritable.get(), outValue, "filelist");
			}

		} else if ( keyText.startsWith("COUNT") ) {
			rowCnt = 0;
			delCnt = 0;
			while ( valIt.hasNext() ) {
				parts = valIt.next().toString().split("\t");
				
				rowCnt += Integer.parseInt(parts[0]);
				delCnt += Integer.parseInt(parts[1]);
			}
			
			if ( rowCnt == delCnt ) {
				outValue.clear();
				outValue.set(keyText.substring(5));
				
				mos.write(NullWritable.get(), outValue, "removefiles");
			}
			
		} else if ( keyText.startsWith("MAX") ) {
			max = 0;
			while ( valIt.hasNext() ) {
				curr = Integer.parseInt(valIt.next().toString());
				if ( curr > max ) {
					max = curr;
				}
			}

			outValue.clear();
			outValue.set(keyText.substring(3) + "\t" + max);
			
			mos.write(NullWritable.get(), outValue, "max");
			
		}
		
	}
	
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}
}
