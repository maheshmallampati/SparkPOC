package com.mcd.gdw.test.daas.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mcd.gdw.test.daas.driver.NextGenUnzipFile;

public class NextGenUnzipFilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	private String[] parts;
    private String fileName;
    private HashMap<Integer,String> removeLines = new HashMap<Integer,String>();
    private int currLine;
    
	@Override
	public void setup(Context context) {
		
		URI[] distPaths;
	    BufferedReader br = null;
	    String[] distPathParts;
	    String line;
	    String splitFileName;
	    Path path;
	    
	    try {
	    	currLine = 0;
			mos = new MultipleOutputs<NullWritable, Text>(context);
	    	
	    	removeLines.clear();
	    	
	    	path = ((FileSplit) context.getInputSplit()).getPath();
	    	splitFileName = path.getName();

	    	parts = splitFileName.split("~");
	    	fileName = parts[0] + "~" + parts[1] + "~" + parts[2];
	    	
	    	distPaths = context.getCacheFiles();
	    
	    	if ( distPaths == null ) {
	    		System.err.println("distpath is null");
	    		System.exit(8);
	    	}
	    	
	    	if ( distPaths != null && distPaths.length > 0 )  {
	    	  
	    		System.out.println(" number of distcache files : " + distPaths.length);
	    	  
	    		for ( int i=0; i<distPaths.length; i++ ) {
			     
	    			System.out.println("distpaths:" + distPaths[i].toString());
		    	  
	    			distPathParts = distPaths[i].toString().split("#");
		    	  
	    			if( distPaths[i].toString().contains(NextGenUnzipFile.DIST_CACHE_REMOVE_ITEMS) ) {
	    				
	    				br  = new BufferedReader(new FileReader("./" + distPathParts[1])); 
	    				
	    				while ( (line=br.readLine()) != null ) {
	    					parts = line.split("\t");

	    					if ( parts[0].equals(splitFileName) ) {
	    						removeLines.put(Integer.parseInt(parts[1]), "");
	    					}
	    				}
	    				
	    				br.close();
	    			}
	    		}
	    	}
	    	
	    } catch (Exception ex) {
	    	ex.printStackTrace(System.err);
	    	System.exit(8);
	    }

	}
	
	@Override
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
	
		currLine++;
		
		if ( !removeLines.containsKey(currLine) ) {
			context.getCounter("DaaS Counters", "Kept Records").increment(1);
			mos.write(NullWritable.get(), value, fileName);
		} else {
			context.getCounter("DaaS Counters", "Skipped Records").increment(1);
		}
	}
	
	@Override 
	protected void cleanup(Context contect) throws IOException, InterruptedException {

		mos.close();

	}

	
}
