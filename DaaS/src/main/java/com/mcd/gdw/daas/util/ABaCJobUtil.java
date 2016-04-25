package com.mcd.gdw.daas.util;

import org.apache.commons.lang.StringUtils;

import com.mcd.gdw.daas.abac.ABaC;

public class ABaCJobUtil {
	
	
	public static void main(String[] args) throws Exception{
		
		String configXmlFile  	 = "";
		String fileType 	     = "";
		String jobGroupCommand   = "";
		String jobCommand     	 = "";
		String jobGroupName	  	 = "";
		String jobName	 	  	 = "";
		String openJobGroupName  = "";
		short jobGroupStatusCd 	 = (short)0;
		String jobGroupStatusDesc="";
		int jobGroupId		 	 = 0;
		int jobId				 = 0;
		int jobSeqNum			 = 0;
		short jobStatusCd		 = (short)0;
		String jobStatusDesc	 = "";
		
		
		

		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-c") && (idx+1) < args.length ) {
				configXmlFile = args[idx+1];
			}else if ( args[idx].equals("-t") && (idx+1) < args.length ) {
					fileType = args[idx+1];
			}else if ( args[idx].equals("-getopenjgrpid") && (idx+1) < args.length ) {
				openJobGroupName = args[idx+1];
			}else if(args[idx].equalsIgnoreCase("-jgrpcommand") && (idx+1) < args.length){
				jobGroupCommand = args[idx+1];
			}else if(args[idx].equalsIgnoreCase("-jcommand") && (idx+1) < args.length){
				jobCommand = args[idx+1];
			}else if(args[idx].equalsIgnoreCase("-jgrpnanme") && (idx+1) < args.length){
				jobGroupName = args[idx+1];
			}else if(args[idx].equalsIgnoreCase("-jname") && (idx+1) < args.length){
				jobName = args[idx+1];
			}else if(args[idx].equalsIgnoreCase("-jgrpid") && (idx+1) < args.length){
				jobGroupId = Integer.parseInt(args[idx+1]);
			}else if(args[idx].equalsIgnoreCase("-jsqnbr") && (idx+1) < args.length){
				jobSeqNum = Integer.parseInt(args[idx+1]);
			}else if(args[idx].equalsIgnoreCase("-jid") && (idx+1) < args.length){
				jobId = Integer.parseInt(args[idx+1]);
			}
			else if(args[idx].equalsIgnoreCase("-jstatusdesc") && (idx+1) < args.length){
				jobStatusDesc = args[idx+1];
			}else if(args[idx].equalsIgnoreCase("-jstatuscd") && (idx+1) < args.length){
				jobStatusCd = (short)Integer.parseInt(args[idx+1]);
			}else if(args[idx].equalsIgnoreCase("-jgrpstatusdesc") && (idx+1) < args.length){
				jobGroupStatusDesc = args[idx+1];
			}else if(args[idx].equalsIgnoreCase("-jgrpstatuscd") && (idx+1) < args.length){
				jobGroupStatusCd = (short)Integer.parseInt(args[idx+1]);
			}
			
		}
		
		DaaSConfig daasConfig = new DaaSConfig(configXmlFile, fileType);
		ABaC abac = null;
		
		try{
			abac = new ABaC(daasConfig);
			int createdJobGroupId = 0;
			int createJobId = 0;
			
			if(StringUtils.isNotBlank(openJobGroupName)){
				
				System.out.println(""+abac.getOpenJobGroupId(openJobGroupName));
			}else{
				if("CREATEJOBGROUP".equalsIgnoreCase(jobGroupCommand)){
					createdJobGroupId = abac.createJobGroup(jobGroupName);
					
					System.out.println(""+createdJobGroupId);
				}else if("CLOSEJOBGROUP".equalsIgnoreCase(jobGroupCommand)){
					abac.closeJobGroup(jobGroupId, jobGroupStatusCd, jobGroupStatusDesc);
				}
				if("CREATEJOB".equalsIgnoreCase(jobCommand)){
					createJobId = abac.createJob(jobGroupId, jobSeqNum, jobName);
					System.out.println(""+createJobId);
				}else if("CLOSEJOB".equalsIgnoreCase(jobCommand)){
					abac.closeJob(jobId, jobStatusCd, jobStatusDesc);
				}
			}
		}catch(Exception ex){
			throw ex;
		}finally{
			
			if(abac != null)
				abac.dispose();
			
		}
	}

}
