package com.mcd.gdw.daas.abac;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

public class ABaCFiles implements Iterable<ABaCFile> {

	private ArrayList<ABaCFile> files = new ArrayList<ABaCFile>();

	public ABaCFiles(File[] files
			        ,ABaCDataTypes dataTypes) {
	
		for (int idx=0; idx < files.length; idx++ ) {
			if ( files[idx].exists() && (files[idx].isFile() && !files[idx].isDirectory()) ) {
				this.files.add(new ABaCFile(files[idx],dataTypes));
			}
		}
		
	}

	public ABaCFiles(File[] files
			        ,int slice
			        ,int slices
			        ,ABaCDataTypes dataTypes) throws Exception {

		if ( slice < 1 ) {
			throw new Exception("Slice must be positive greater than or equal to 1.");
		}
		
		if ( slices < 1 ) {
			throw new Exception("Slices must be positive greater than or equal to 1.");
		}
		
		if ( slice > slices ) {
			throw new Exception("Slice (" + slice + ") must be positive greater than or equal to 1 and less than or equal to slices (" + slices + ").");
		}
		
		int sliceSize = files.length / slices;
		int idx;
		int start = ((slice-1) * sliceSize);
		int max;
	
		if ( sliceSize < 1 ) {
			throw new Exception("Slices (" + slices + ") too large for number of items (" + files.length + ")");
		}

		if ( slice == slices ) {
			max = files.length;
		} else {
			max = start + sliceSize;
	    }
		
		for (idx=start; idx < max; idx++ ) {
			if ( files[idx].exists() && (files[idx].isFile() && !files[idx].isDirectory()) ) {
				this.files.add(new ABaCFile(files[idx],dataTypes));
			}
		}
	}
	
	@Override
	public Iterator<ABaCFile> iterator() {
		
		return(files.iterator());
		
	}
	
	public void clear() {
		
		files.clear();
		
	}
	
	public int size() {
		
		return(files.size());
		
	}
	
}
