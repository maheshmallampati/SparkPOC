package com.mcd.gdw.daas.util;

import java.math.BigDecimal;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DWFileDBUtil {

	public static void main(String[] args) {
		try{
		DWFileDBUtil  dwFileDBUtil = new DWFileDBUtil();
		
		String[]  values = new String[15];
 
		String dwFileName 		= "TestFName1234";
		String fileIncmOutgCd   = "A";
		String fMktOrgTS		= "2012-06-05 07:43:10";
		String fDWArrTS			= "2012-06-06 07:43:10";
		String fPRCSStTS		= null;
		String fPRCSEndTS	    = null;
		String mcdGblLcatIDNU	= "1234567890113";
		String terrCd			= "840";
		String lgcyLclRfrDefCd  = "lgcyCd";
		String fileSz			= "1024";
		String filePathDs		= "path";
		String fileRecCntQt		= "1234";
		String calDt			= "2013-10-24";
		String dwdataTypId		= "96";
		String dwAudtStusTypeId	= "3";
		String dbServerName		= "mcdeagpapp235";
		String userName			= "mc32445";
		String password			= "bigm@c11";
		
		RDBMS rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,dbServerName,userName,password);
		
		dwFileDBUtil.insertToDWFile(dwFileName,fileIncmOutgCd,fMktOrgTS,fDWArrTS,fPRCSStTS,fPRCSEndTS,mcdGblLcatIDNU,terrCd,lgcyLclRfrDefCd
				,fileSz,filePathDs,fileRecCntQt,calDt,dwdataTypId,dwAudtStusTypeId, rdbmsutil);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm");
	SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd");

//	public void insertToDWFile(String dwFileName,String fileIncmOutgCd,String fMktOrgTSStr,
//			String fDWArrTSStr,String fPRCSStTSStr,String fPRCSEndTSStr,String mcdGblLcatIDNUStr,String terrCdStr,
//			String lgcyLclRfrDefCd,
//			String fileSzStr,String filePathDs,String fileRecCntQtStr,String calDtStr,
//			String dwdataTypIdStr,String dwAudtStusTypeIdStr,String dbServerName,String userName,String password){
		
		public void insertToDWFile(String dwFileName,String fileIncmOutgCd,String fMktOrgTSStr,
				String fDWArrTSStr,String fPRCSStTSStr,String fPRCSEndTSStr,String mcdGblLcatIDNUStr,String terrCdStr,
				String lgcyLclRfrDefCd,
				String fileSzStr,String filePathDs,String fileRecCntQtStr,String calDtStr,
				String dwdataTypIdStr,String dwAudtStusTypeIdStr,RDBMS rdbmsutil){		
		Timestamp fMktOrgTS    = null;
		Timestamp fDWArrTS 	   = null;
		Timestamp fPRCSStTS    = null;
		Timestamp fPRCSEndTS   = null;
		BigDecimal mcdGblLcatIDNU = null;
		int fileRecCntQt = 0;
		Short terrCd = null;
		float fileSz = 0;
		java.sql.Date calDt = null;
		Short dwdataTypId = null;
		Short dwAudtStusTypeId = null;
		
		try{
			Date fMktOrgDt;
			
			if(fMktOrgTSStr != null){
				try{
					fMktOrgDt = sdf1.parse(fMktOrgTSStr);
				}catch(Exception ex){
					fMktOrgDt = sdf2.parse(fMktOrgTSStr);
				}
			fMktOrgTS = new Timestamp(fMktOrgDt.getTime());
			}
			
			if(fDWArrTSStr != null){
				Date fDWArrDt;
				try{
					fDWArrDt = sdf1.parse(fDWArrTSStr);
				}catch(Exception ex){
					fDWArrDt = sdf2.parse(fDWArrTSStr);
				}
				fDWArrTS = new Timestamp(fDWArrDt.getTime());
			}
			
			
			if(fPRCSStTSStr != null){
				Date fPRCSStDt;
				try{
					fPRCSStDt = sdf1.parse(fPRCSStTSStr);
				}catch(Exception ex){
					fPRCSStDt = sdf2.parse(fPRCSStTSStr);
				}
				fPRCSStTS = new Timestamp(fPRCSStDt.getTime());
			}
		
			if(fPRCSEndTSStr != null){
				Date fPRCSEndDt;
				try{
					fPRCSEndDt = sdf1.parse(fPRCSEndTSStr);
				}catch(Exception ex){
					fPRCSEndDt = sdf2.parse(fPRCSEndTSStr);
				}
				fPRCSEndTS = new Timestamp(fPRCSEndDt.getTime());
			}
			
			
			if(mcdGblLcatIDNUStr != null){
				mcdGblLcatIDNU = new BigDecimal(mcdGblLcatIDNUStr);
			}
			
			if(terrCdStr != null){
				terrCd = new Short(terrCdStr);
			}
			
			if(fileSzStr != null){
				fileSz = Float.valueOf(fileSzStr);
			}
			
			if(fileRecCntQtStr != null){
				fileRecCntQt = Integer.parseInt(fileRecCntQtStr);
			}
			
			if(calDtStr != null){
				calDt = new java.sql.Date(sdf3.parse(calDtStr).getTime());
			}
			if(dwdataTypIdStr != null){
				dwdataTypId = new Short(dwdataTypIdStr);
			}
			
			if(dwAudtStusTypeIdStr != null){
				dwAudtStusTypeId = new Short(dwAudtStusTypeIdStr);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
//		insertToDWFileWithCorrectTypes(dwFileName,fileIncmOutgCd,fMktOrgTS,fDWArrTS,fPRCSStTS,fPRCSEndTS,
//				mcdGblLcatIDNU,terrCd,lgcyLclRfrDefCd,fileSz,filePathDs,fileRecCntQt,calDt,dwdataTypId,dwAudtStusTypeId,
//				dbServerName, userName, password);
		
		insertToDWFileWithCorrectTypes(dwFileName,fileIncmOutgCd,fMktOrgTS,fDWArrTS,fPRCSStTS,fPRCSEndTS,
				mcdGblLcatIDNU,terrCd,lgcyLclRfrDefCd,fileSz,filePathDs,fileRecCntQt,calDt,dwdataTypId,dwAudtStusTypeId,
				rdbmsutil);
	}
	
//	public void insertToDWFileWithCorrectTypes(String dwFileName,String fileIncmOutgCd,Timestamp fMktOrgTS,
//			Timestamp fDWArrTS,Timestamp fPRCSStTS,Timestamp fPRCSEndTS,BigDecimal mcdGblLcatIDNU,short terrCd,
//			String lgcyLclRfrDefCd,
//			float fileSz,String filePathDs,int fileRecCntQt,Date calDt,short dwdataTypId,short dwAudtStusTypeId,
//			String dbServerName,String userName,String password ){
		public void insertToDWFileWithCorrectTypes(String dwFileName,String fileIncmOutgCd,Timestamp fMktOrgTS,
				Timestamp fDWArrTS,Timestamp fPRCSStTS,Timestamp fPRCSEndTS,BigDecimal mcdGblLcatIDNU,short terrCd,
				String lgcyLclRfrDefCd,
				float fileSz,String filePathDs,int fileRecCntQt,Date calDt,short dwdataTypId,short dwAudtStusTypeId,
				RDBMS rdbmsutil ){		
//		RDBMS rdbmsutil = null;
		try{
			
//			rdbmsutil = new RDBMS(RDBMS.ConnectionType.SQLServer,dbServerName,userName,password);
			
		
			
			
			
			String insertSql = " insert into ABaC.dbo.DW_File (DW_FILE_NA," +
															  "FILE_INCM_OUTG_CD,FILE_MKT_OGIN_TS," +
															  "FILE_DW_ARRV_TS,FILE_PRCS_STRT_TS," +
															  "FILE_PRCS_END_TS,MCD_GBAL_LCAT_ID_NU," +
															  "TERR_CD,LGCY_LCL_RFR_DEF_CD,FILE_SIZE_NU," +
															  "FILE_PATH_DS,FILE_REC_CNT_QT," +
															  "CAL_DT,DW_DATA_TYP_ID,DW_AUDT_STUS_TYP_ID"+
															  ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
			
			rdbmsutil.setPreparedStatement(insertSql);
			
			rdbmsutil.setAutoCommit(false);
			
			int updCount = 0;
			
			updCount = rdbmsutil.addBatch(
                    dwFileName 
                    ,fileIncmOutgCd
                    ,fMktOrgTS
                    ,fDWArrTS
                    ,fPRCSStTS
                    ,fPRCSEndTS
                    ,mcdGblLcatIDNU
                    ,terrCd
                    ,lgcyLclRfrDefCd
                    ,fileSz
                    ,filePathDs
                    ,fileRecCntQt
                    ,calDt
                    ,dwdataTypId
                    ,dwAudtStusTypeId);


			
		
			 updCount = rdbmsutil.finalizeBatch();
			 System.out.println(updCount);
			 rdbmsutil.commit();
			    
			
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				if(rdbmsutil != null)
					rdbmsutil.dispose();
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
}
