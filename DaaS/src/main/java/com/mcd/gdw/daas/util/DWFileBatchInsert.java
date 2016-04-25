package com.mcd.gdw.daas.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DWFileBatchInsert {
	
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd");
		
       public static void main(String[] args){
    	   
    	   try{
    		   
    		   String configXmlFile = "/config.xml";
	    	   String dwfileDatafilePath = null;
	    	   
	    	   for ( int idx=0; idx < args.length; idx++ ) {
		   			if ( args[idx].equalsIgnoreCase("-c") && (idx+1) < args.length ) {
		   				configXmlFile = args[idx+1];
		   			}else 
		   			if ( args[idx].equalsIgnoreCase("-i") && (idx+1) < args.length ) {
		   				dwfileDatafilePath = args[idx+1].trim();
		   			}
	    	   }
	    	   
	    	   
	    	  
	    	 
	    	   DWFileBatchInsert dwFileBatchInsert = new DWFileBatchInsert();
	    	   dwFileBatchInsert.readFileandInsertRowstoDWFile(configXmlFile,dwfileDatafilePath);
	    	   
	    	   System.out.println( " calling exit with 100 ");
	    	   System.exit(100);
    	   }catch(Exception ex){
    		   ex.printStackTrace();
    		   System.out.println( " calling exit with -1");
    		   System.exit(10);
    	   }
       }
       
     
       
       public int readFileandInsertRowstoDWFile(String configXmlFile,String dwfileDatafilePath) throws Exception{
    	   
    	   BufferedReader br = null;
    	   RDBMS rdbmsutil = null;
    	    try {
    	    	
    	    	br =  new BufferedReader(new FileReader(dwfileDatafilePath));
    	        StringBuilder sb = new StringBuilder();
    	        String line = null;
    	        String[] lineparts;
    	        
    	        DaaSConfig daasConfig = new DaaSConfig(configXmlFile);
    	       
    	        
//    	        String dbServerName		= "mcdeagpapp235";
//        		String userName			= "mc32445";
//        		String password			= "";
        		
//        		rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,dbServerName,userName,password);
//    	        System.out.println(" user " + daasConfig.abacSqlServerUserId()+ " password " +daasConfig.abacSqlServerPassword());
        		rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,daasConfig.abacSqlServerServerName(),daasConfig.abacSqlServerUserId(),daasConfig.abacSqlServerPassword());
        		
//        		String insertSql = " insert into "+daasConfig.abacSqlServerDb()+".DW_File (DW_FILE_ID,DW_FILE_NA," +
//						  "FILE_INCM_OUTG_CD,FILE_MKT_OGIN_TS," +
//						  "FILE_DW_ARRV_TS,FILE_PRCS_STRT_TS," +
//						  "FILE_PRCS_END_TS,MCD_GBAL_LCAT_ID_NU," +
//						  "TERR_CD,LGCY_LCL_RFR_DEF_CD,FILE_SIZE_NU," +
//						  "FILE_PATH_DS,FILE_REC_CNT_QT," +
//						  "CAL_DT,DW_DATA_TYP_ID,DW_AUDT_STUS_TYP_ID"+
//						  ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        		
        		String insertSql = " insert into "+daasConfig.abacSqlServerDb()+".DW_File (DW_FILE_NA," +
						  "FILE_INCM_OUTG_CD,FILE_MKT_OGIN_TS," +
						  "FILE_DW_ARRV_TS,FILE_PRCS_STRT_TS," +
						  "FILE_PRCS_END_TS,MCD_GBAL_LCAT_ID_NU," +
						  "TERR_CD,LGCY_LCL_RFR_DEF_CD,FILE_SIZE_NU," +
						  "FILE_PATH_DS,FILE_REC_CNT_QT," +
						  "CAL_DT,DW_DATA_TYP_ID,DW_AUDT_STUS_TYP_ID"+
						  ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

        		rdbmsutil.setPreparedStatement(insertSql);

        		rdbmsutil.setAutoCommit(false);
        		
        		int indx = 0;
    	        while ((line = br.readLine()) != null) {
    	        	
    	        	lineparts = line.split("\\|");
    	        	
//    	        	String DW_FILE_ID				 = lineparts[indx++].trim();
    	        	String DW_FILE_NA                = lineparts[indx++].trim();
                    String FILE_PATH_DS              = lineparts[indx++].trim();
                    String FILE_MKT_OGIN_TS          = lineparts[indx++].trim();
                    String FILE_DW_ARRV_TS           = lineparts[indx++].trim();
                    String MCD_GBAL_LCAT_ID_NU       = lineparts[indx++].trim();
                    String TERR_CD                   = lineparts[indx++].trim();
                    String LGCY_LCL_RFR_DEF_CD       = lineparts[indx++].trim();
                    String FILE_SIZE_NU			 	 = lineparts[indx++].trim();
                    String FILE_REC_CNT_QTY          = lineparts[indx++].trim();
                    String CAL_DT                    = lineparts[indx++].trim();
                    String DW_DATA_TYP_ID            = lineparts[indx++].trim();
                    String DW_AUDT_STUS_TYP_ID       = lineparts[indx++].trim();
                    String FILE_INCM_OUTG_CD         = lineparts[indx++].trim();
//                    String FILE_RSMT_DATA_FL		 = lineparts[13].trim();
                    
                    indx = 0;
                   
            		
//                    addToBatch(DW_FILE_ID,DW_FILE_NA, FILE_PATH_DS, FILE_MKT_OGIN_TS, FILE_DW_ARRV_TS, MCD_GBAL_LCAT_ID_NU, TERR_CD, LGCY_LCL_RFR_DEF_CD, FILE_SIZE_NU, 
//                    		FILE_REC_CNT_QTY, CAL_DT, DW_DATA_TYP_ID, DW_AUDT_STUS_TYP_ID, FILE_INCM_OUTG_CD, rdbmsutil);
                    addToBatch(DW_FILE_NA, FILE_PATH_DS, FILE_MKT_OGIN_TS, FILE_DW_ARRV_TS, MCD_GBAL_LCAT_ID_NU, TERR_CD, LGCY_LCL_RFR_DEF_CD, FILE_SIZE_NU, 
                    		FILE_REC_CNT_QTY, CAL_DT, DW_DATA_TYP_ID, DW_AUDT_STUS_TYP_ID, FILE_INCM_OUTG_CD, rdbmsutil);
    	        }
    	        
    	     
    	        int updCount = rdbmsutil.finalizeBatch();
  			 	System.out.println(" Total number of rows inserted into database " + updCount);
  			 	rdbmsutil.commit();
    	        
    	    }catch(Exception ex){
    	    	ex.printStackTrace();
    	    	throw ex;
    	    
    	    }finally{
    	    	
    	    	try{
    	    		if(br != null)
    	    			br.close();
    	    		if(rdbmsutil != null)
						rdbmsutil.dispose();
    	    		
    	    	}catch(Exception ex){
    	    		ex.printStackTrace();
    	    		throw ex;
    	    	}
    	    	
    	    	
    	    }
    	   
    	   
    	   return 0;
       }
//       public void addToBatch(String DW_FILE_ID_STR,String DW_FILE_NA, String FILE_PATH_DS, String FILE_MKT_OGIN_TS_STR, String FILE_DW_ARRV_TS_STR, String MCD_GBAL_LCAT_ID_NU_STR, 
//    		   String TERR_CD_STR, String LGCY_LCL_RFR_DEF_CD, String FILE_SIZE_NU_STR, String FILE_REC_CNT_QTY_STR, String CAL_DT_STR, String DW_DATA_TYP_ID_STR, 
//    		   String DW_AUDT_STUS_TYP_ID_STR, String FILE_INCM_OUTG_CD,  RDBMS rdbmsutil){	
       
       public void addToBatch(String DW_FILE_NA, String FILE_PATH_DS, String FILE_MKT_OGIN_TS_STR, String FILE_DW_ARRV_TS_STR, String MCD_GBAL_LCAT_ID_NU_STR, 
    		   String TERR_CD_STR, String LGCY_LCL_RFR_DEF_CD, String FILE_SIZE_NU_STR, String FILE_REC_CNT_QTY_STR, String CAL_DT_STR, String DW_DATA_TYP_ID_STR, 
    		   String DW_AUDT_STUS_TYP_ID_STR, String FILE_INCM_OUTG_CD,  RDBMS rdbmsutil) throws Exception{		
    	   
    	   try{
    		   
//    		    int DW_FILE_ID = 0;
				Timestamp FILE_MKT_OGIN_TS     = null;
				Timestamp FILE_DW_ARRV_TS 	   = null;
				Timestamp fPRCSStTS    = null;
				Timestamp fPRCSEndTS   = null;
				BigDecimal MCD_GBAL_LCAT_ID_NU = null;
				int FILE_REC_CNT_QTY = 0;
				Short TERR_CD = null;
				float FILE_SIZE_NU = 0;
				java.sql.Date CAL_DT = null;
				Short DW_DATA_TYP_ID = null;
				Short DW_AUDT_STUS_TYP_ID = null;
				Date fMktOrgDt;
				try{
					
//					if(DW_FILE_ID_STR != null && !DW_FILE_ID_STR.isEmpty()){
//						DW_FILE_ID = Integer.parseInt(DW_FILE_ID_STR);
//					}
					if(FILE_MKT_OGIN_TS_STR != null && !FILE_MKT_OGIN_TS_STR.isEmpty()){
						try{
							fMktOrgDt = sdf1.parse(FILE_MKT_OGIN_TS_STR);
						}catch(Exception ex){
							fMktOrgDt = sdf2.parse(FILE_MKT_OGIN_TS_STR);
						}
						FILE_MKT_OGIN_TS = new Timestamp(fMktOrgDt.getTime());
					}
					
					if(FILE_DW_ARRV_TS_STR != null && !FILE_DW_ARRV_TS_STR.isEmpty()){
						Date fDWArrDt;
						try{
							fDWArrDt = sdf1.parse(FILE_DW_ARRV_TS_STR);
						}catch(Exception ex){
							fDWArrDt = sdf2.parse(FILE_DW_ARRV_TS_STR);
						}
						FILE_DW_ARRV_TS = new Timestamp(fDWArrDt.getTime());
					}
					
				
					
					if(MCD_GBAL_LCAT_ID_NU_STR != null && !MCD_GBAL_LCAT_ID_NU_STR.isEmpty()){
						MCD_GBAL_LCAT_ID_NU = new BigDecimal(MCD_GBAL_LCAT_ID_NU_STR);
					}
					
					if(TERR_CD_STR != null && !TERR_CD_STR.isEmpty()){
						TERR_CD = new Short(TERR_CD_STR);
					}
					
					if(FILE_SIZE_NU_STR != null && !FILE_SIZE_NU_STR.isEmpty()){
						FILE_SIZE_NU = Float.valueOf(FILE_SIZE_NU_STR);
					}
					
					if(FILE_REC_CNT_QTY_STR != null && !FILE_REC_CNT_QTY_STR.isEmpty()){
						FILE_REC_CNT_QTY = Integer.parseInt(FILE_REC_CNT_QTY_STR);
					}
					
					if(CAL_DT_STR != null && !CAL_DT_STR.isEmpty()){
						CAL_DT = new java.sql.Date(sdf3.parse(CAL_DT_STR).getTime());
					}
					if(DW_DATA_TYP_ID_STR != null && !DW_DATA_TYP_ID_STR.isEmpty()){
						DW_DATA_TYP_ID = new Short(DW_DATA_TYP_ID_STR);
					}
					
					if(DW_AUDT_STUS_TYP_ID_STR != null && !DW_AUDT_STUS_TYP_ID_STR.isEmpty()){
						DW_AUDT_STUS_TYP_ID = new Short(DW_AUDT_STUS_TYP_ID_STR);
					}
				}catch(Exception ex){
					ex.printStackTrace();
				}
		
				if(rdbmsutil == null)
					System.out.println(" rdbmsutil is null *************");
				
//				rdbmsutil.addBatch(
//						DW_FILE_ID
//					   ,DW_FILE_NA 
//		               ,FILE_INCM_OUTG_CD
//		               ,FILE_MKT_OGIN_TS
//		               ,FILE_DW_ARRV_TS
//		               ,fPRCSStTS
//		               ,fPRCSEndTS
//		               ,MCD_GBAL_LCAT_ID_NU
//		               ,TERR_CD
//		               ,LGCY_LCL_RFR_DEF_CD
//		               ,FILE_SIZE_NU
//		               ,FILE_PATH_DS
//		               ,FILE_REC_CNT_QTY
//		               ,CAL_DT
//		               ,DW_DATA_TYP_ID
//		               ,DW_AUDT_STUS_TYP_ID);
				
				rdbmsutil.addBatch(
						DW_FILE_NA 
		               ,FILE_INCM_OUTG_CD
		               ,FILE_MKT_OGIN_TS
		               ,FILE_DW_ARRV_TS
		               ,fPRCSStTS
		               ,fPRCSEndTS
		               ,MCD_GBAL_LCAT_ID_NU
		               ,TERR_CD
		               ,LGCY_LCL_RFR_DEF_CD
		               ,FILE_SIZE_NU
		               ,FILE_PATH_DS
		               ,FILE_REC_CNT_QTY
		               ,CAL_DT
		               ,DW_DATA_TYP_ID
		               ,DW_AUDT_STUS_TYP_ID);
		    	   }catch(Exception ex){
		    		   ex.printStackTrace();
		    		   throw ex;
		    	   }
		
	}
       
       
       
       
       
}
