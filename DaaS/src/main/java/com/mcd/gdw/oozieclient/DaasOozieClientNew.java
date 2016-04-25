package com.mcd.gdw.oozieclient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

//AWS START
//import org.apache.oozie.client.CoordinatorJob;
//import org.apache.oozie.client.OozieClient;
//import org.apache.oozie.client.WorkflowAction;
//import org.apache.oozie.client.WorkflowJob;
//AWS END

public class DaasOozieClientNew {
	
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	public static void main(String[] args){
		
		FileInputStream fs = null;
		try{
			
			//http://192.65.208.63:11000/oozie
//AWS START
//		OozieClient ozclient = new OozieClient(args[0]);
//AWS END
		
		Properties props = new Properties();
		fs = new FileInputStream(args[1]);//"job.properties");
		props.load(fs);
		
		if( ((String)props.get("SET_TDA_EXTRACT_INPUT_PATHS_TO_GOLD_LAYER")).equalsIgnoreCase("true")){
			String rundate = args[2];
			String prevDate = null;
			
			if(rundate == null || rundate.isEmpty()){
				Calendar yesterday = Calendar.getInstance();
				yesterday.add(Calendar.DATE,-1);
			
				Date ysDt = yesterday.getTime();
				prevDate = sdf.format(ysDt);
			}else{
				prevDate = rundate;
			}
			
			
			String tdaExtractsStldInputPath = props.getProperty("TDA_EXTRACT_STLD_BASE_INPUT_PATH")+"/"+prevDate;
			String tdaExtractsDetailedSOSInputPath = props.getProperty("TDA_EXTRACT_DTLSOS_BASE_INPUT_PATH")+"/"+prevDate;
			
			props.setProperty("TDA_EXTRACT_STLD_INPUT_PATH", tdaExtractsStldInputPath);
			props.setProperty("TDA_EXTRACT_SOS_INPUT_PATH", tdaExtractsDetailedSOSInputPath);
		}
	
		fs.close();
		
//		System.out.println(props.toString());
		
	
		
//AWS START
		 
//		String jobId = ozclient.run(props);
		
//		CoordinatorJob wfj = ozclient.getCoordJobInfo(jobId);
		
//		while(ozclient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING){
//			System.out.println(" job running " + jobId + " getid " + wfj.getId());
//			try{
//				Thread.sleep(10*1000);
//			}catch(Exception ex){
//				ex.printStackTrace();
//			}
//		}
//		
//		wfj = ozclient.getCoordJobInfo(jobId);
//		
//		if(wfj.getStatus() == org.apache.oozie.client.Job.Status.SUCCEEDED){
//			System.out.println(" job completed successfully " + wfj.getId());
//		}

//AWS END
		
//		List<WorkflowAction> wfalst = wfj.getActions();
		
		
//		System.out.println("Name\tType\tStatus\tStart Time\tEnd Time\tExternal Id\tExternal Status");
//		
//		for(WorkflowAction wfact:wfalst){
//			
//			System.out.println(wfact.getName()+"\t"+wfact.getType()+"\t"+wfact.getStatus() + "\t"+
//							   wfact.getStartTime() + "\t"+wfact.getEndTime() +"\t"+
//							   wfact.getExternalId() + "\t"+wfact.getExternalStatus());
//		}
//		
//		for(WorkflowAction wfact:wfalst){
//			
//			System.out.println("Name : "+wfact.getName()+" : Type : "+wfact.getType()+" : Status : "+wfact.getStatus() + " : Start Time : "+
//							   wfact.getStartTime() + " : End Time : "+wfact.getEndTime() +" : External Id : "+
//							   wfact.getExternalId() + " : External Status : "+wfact.getExternalStatus());
//		}
//		
		
		
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				if(fs != null)
					fs.close();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
	}
	
	
}
