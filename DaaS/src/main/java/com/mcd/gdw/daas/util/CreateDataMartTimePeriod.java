package com.mcd.gdw.daas.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;



public class CreateDataMartTimePeriod {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File outputFile = new File(System.getProperty("oozie.action.output.properties"));
		Properties outputProp = new Properties();
		String refreshTimePeriod ="";
		Date today = new Date();
		Calendar cal = new GregorianCalendar();
		cal.setTime(today);
		System.out.println("Today is "+today);
		for ( int idx=0; idx < args.length; idx++ ) {
			if ( args[idx].equals("-refreshTimePeriod") && (idx+1) < args.length ) {
				refreshTimePeriod = args[idx+1];
			}
		}
		int rtp= Integer.parseInt(refreshTimePeriod);
		cal.add(Calendar.DATE, -rtp );
		Date today120 = cal.getTime();
		//cal.add(Calendar.DATE, -today120.getDay());// Logic for calculating the last sunday before the date
		DateFormat df3 = new SimpleDateFormat("yyyy-MM-dd");
		String dMartRefreshDate =df3.format(cal.getTime());
		System.out.println("dMartRefreshDate is -- "+dMartRefreshDate);
		//outputProp.setProperty("MYDATE", "2015-11-28");
		outputProp.setProperty("MYDATE", dMartRefreshDate);
		OutputStream oStream;
		try {
			oStream = new FileOutputStream(outputFile);
			outputProp.store(oStream , "");
			oStream.close();
		} catch (Exception e) {
	//		 TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(outputFile.getAbsolutePath());

	}

}
