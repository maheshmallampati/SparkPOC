package com.mcd.gdw.daas.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class PathFilterByFileName implements PathFilter{

	String filenamePattern;
	public PathFilterByFileName(String filenamePattern){
		this.filenamePattern = filenamePattern.toUpperCase();
	}
	
	@Override
	public boolean accept(Path pathname) {
		
		String fileName = pathname.getName().toUpperCase();
		if(fileName.startsWith(filenamePattern))
			return true;
		return false;
	}
	

}
