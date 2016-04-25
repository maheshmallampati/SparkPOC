package com.mcd.gdw.daas.util;

public class TimeUtil {

	
	public static String getTimeinMMSS(long milliseconds){
		
		 long seconds = (milliseconds) / 1000; 
		 long minutes = seconds / 60; 
		 String timeMMSS = String.format("%1$02d%2$02d", minutes, Math.round(seconds % 60));
		
		 if(timeMMSS != null){
			 if(timeMMSS.indexOf("-") == 0){
				 timeMMSS = timeMMSS.replaceAll("-", "");
				 return "-"+timeMMSS;
			 }
		 }
		 return timeMMSS;
		  
	}
}