package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mcd.gdw.test.daas.driver.FindFileIds;

public class FindFileIdsReducer extends Reducer<Text, Text, Text, Text> {

	private class FileTypeCounts {
		
		private int stldCount = 0;
		private int detailedSosCount = 0;
		private int menuItemCount = 0;
		private int securityDataCount = 0;
		private int storeDbCount = 0;
		private int productDbCount = 0;
		private int errorCount = 0;
		private HashMap<String,String> partFileList = new HashMap<String,String>(); 
		
		public void init() {

			stldCount = 0;
			detailedSosCount = 0;
			menuItemCount = 0;
			securityDataCount = 0;
			storeDbCount = 0;
			productDbCount = 0;
			errorCount = 0;
			partFileList.clear(); 
			
		}
		
		public void addFileType(String partFile
				               ,String xmlFileType) {
			
			if ( !partFileList.containsKey(partFile) ) {
				partFileList.put(partFile, partFile);
			}
			
			if ( xmlFileType.equals("STLD") ) {
				stldCount++;
			} else if ( xmlFileType.equals("DetailedSOS") ) {
				detailedSosCount++;
			} else if ( xmlFileType.equals("MenuItem") ) {
				menuItemCount++;
			} else if ( xmlFileType.equals("SecurityData") ) {
					securityDataCount++;
			} else if ( xmlFileType.equals("Store-Db") ) {
				storeDbCount++;
			} else if ( xmlFileType.equals("Product-Db") ) {
				productDbCount++;
			} else {
				errorCount++;
			}
			
		}
		
		public String toString() {
			
			return(stldCount + "\t" + 
			       detailedSosCount + "\t" + 
				   menuItemCount + "\t" + 
			       securityDataCount + "\t" +
			       storeDbCount + "\t" + 
			       productDbCount + "\t" +
			       partFileList.size() + "\t" + 
			       errorCount);
			
		}
	}
	
	//private HashMap<String,FileTypeCounts> fileCounts = new HashMap<String,FileTypeCounts>();
	private String[] parts;
	private FileTypeCounts counts = new FileTypeCounts();
	
	private Text valueOut = new Text();
	
	@Override  
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		counts.init();
		
		for (Text value : values ) {
			
			parts = value.toString().split("\\t");
		
			counts.addFileType(parts[FindFileIds.POS_FILE_NAME], parts[FindFileIds.POS_FILE_TYPE]);

			valueOut.clear();
			valueOut.set("DETAIL\t" + parts[FindFileIds.POS_FILE_TYPE] + "\t" + parts[FindFileIds.POS_FILE_NAME]);
				
			context.write(key, valueOut);

			//if ( fileCounts.containsKey(parts[FindFileIds.POS_FILE_NAME]) ) {
			//	itm = fileCounts.get(parts[FindFileIds.POS_FILE_NAME]);
			//} else {
			//	itm = new FileTypeCounts();
			//}
            //
			//itm.addFileType(parts[FindFileIds.POS_FILE_TYPE]);
			//fileCounts.put(parts[FindFileIds.POS_FILE_NAME], itm);
			
		}
		
		//for (Map.Entry<String, FileTypeCounts> entry : fileCounts.entrySet()) {
		valueOut.clear();
		valueOut.set("SUMMARY\t" + counts.toString());
			
		context.write(key, valueOut);
		//}

	}
}
