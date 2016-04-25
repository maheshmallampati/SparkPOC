import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;

import com.mcd.gdw.daas.abac.ABaC;
import com.mcd.gdw.daas.abac.ABaCList;
import com.mcd.gdw.daas.abac.ABaCListItem;
import com.mcd.gdw.daas.driver.MergeToFinal;
import com.mcd.gdw.daas.util.DaaSConfig;


public class TestABaCRDBMS {
	
	public static void main(String[] args){
		
		BufferedReader br = null;
		ABaCList abac2List = null;
		try{
		
			DaaSConfig daasConfig = new DaaSConfig("C:/Users/mc32445/Desktop/DaaS/dev_config.xml");
			String fileName = "C:/Users/mc32445/Desktop/DaaS/FileStatus-r-00021";
			
			ABaC abac = new ABaC(daasConfig);
			FileReader fr = new FileReader(new File(fileName));
			
			br= new BufferedReader(fr) ;
			
			String line;
			line=br.readLine();

			ABaCListItem  abac2ListItem = null;
			MergeToFinal mf = new MergeToFinal();
			Timestamp starttm = mf.getJobGroupStartTime(1364, daasConfig);
			
			abac2List = new ABaCList(daasConfig.fileFileSeparatorCharacter());
			
			while (line != null){
			
				abac2ListItem = ABaC.deserializeListItemFromHexString(line);
				
				abac2ListItem.setFileProcessStartTimestamp(starttm);
				
				abac2List.addItem(abac2ListItem);
			
				
				
				
				
				line=br.readLine();
			}
			
			abac.updateFileList(abac2List);
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

}
