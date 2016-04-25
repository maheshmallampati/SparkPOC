package com.mcd.gdw.daas.mapreduce;

/**
* Copyright 2011 Michael Cutler <m@cotdp.com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.mortbay.jetty.EofException;

/**
* This RecordReader implementation extracts individual files from a ZIP
* file and hands them over to the Mapper. The "key" is the decompressed
* file name, the "value" is the file contents.
*/
public class ZipFileRecordReader
    extends RecordReader<Text, BytesWritable>
{
    /** InputStream used to read the ZIP file from the FileSystem */
    private FSDataInputStream fsin;

    /** ZIP file parser/decompresser */
    private ZipInputStream zip;

    /** Uncompressed file name */
    private Text currentKey;

    /** Uncompressed file contents */
    private BytesWritable currentValue;
//    private Text currentValue = new Text();

    /** Used to indicate progress */
    private boolean isFinished = false;

    long MAX_FILE_SIZE = 0;
    String skipFilesonSize = "true";
    
    private String zipfilename; 
    TaskAttemptContext taskAttemptContext;
    
    Path path = null;
/**
* Initialise and open the ZIP file from the FileSystem
*/
    @Override
    public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
        throws IOException, InterruptedException
    {
    	
    	this.taskAttemptContext = taskAttemptContext;
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        path = split.getPath();
        FileSystem fs = path.getFileSystem( conf );
        zipfilename = path.getName();
        // Open the stream
        fsin = fs.open( path );
        zip = new ZipInputStream( fsin );
        
        skipFilesonSize = taskAttemptContext.getConfiguration().get("skipFilesonSize");
        
        if(taskAttemptContext.getConfiguration().get("MAX_FILE_SIZE") != null )
        	MAX_FILE_SIZE = Long.parseLong(taskAttemptContext.getConfiguration().get("MAX_FILE_SIZE"));
        
       
    }

/**
* This is where the magic happens, each ZipEntry is decompressed and
* readied for the Mapper. The contents of each file is held *in memory*
* in a BytesWritable object.
*
* If the ZipFileInputFormat has been set to Lenient (not the default),
* certain exceptions will be gracefully ignored to prevent a larger job
* from failing.
*/
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] bytesfrombos = null;
    byte[] temp = null;
    boolean isFirstTime = true;
    @Override
    public boolean nextKeyValue()
        throws IOException, InterruptedException
    {
    	
//      BytesWritable bw = new BytesWritable();
     
      if(bos == null)
   	   bos = new ByteArrayOutputStream();
      else{
   	   bos.reset();
   	   bos = null;
   	   bos = new ByteArrayOutputStream();
      }
       if(bytesfrombos != null){
       	Arrays.fill(bytesfrombos, (byte)0);
       	bytesfrombos = null;
       }
       if(temp != null){
    	   Arrays.fill(temp, (byte)0);
       }else{
    	   temp = new byte[8192];
       }
       
       
    	
        ZipEntry entry = null;
        try
        {
            entry = zip.getNextEntry();
        }
        catch ( ZipException e )
        {
            if ( ZipFileInputFormat.getLenient() == false ){
            	e.printStackTrace();
            	currentValue = new BytesWritable( ("CORRUPT_FILE:File "+ path.getName() + " is corrupt").getBytes());
            	return true;
//            	throw e;
            }
        }catch(Exception ex){
        	ex.printStackTrace();
        }
        
        // Sanity check
        if ( entry == null )
        {
        
        	
            
            if(isFirstTime){
            	currentValue = new BytesWritable( ("CORRUPT_FILE:File "+ path.getName() + " is corrupt").getBytes());
                isFinished = true;
            	isFirstTime = false;
            	return true;
            }else
            	return false;
        }else{
        	isFirstTime = false;
        }
        
      
        // Filename
        currentKey = new Text( entry.getName());
        
      
        
//        if(! (entry.getName().startsWith("POS_")) && ! (entry.getName().startsWith("TL")) && ! (entry.getName().startsWith("POS35")) &&  !entry.getName().contains("Content_Types")){
//        	System.out.println( " file " + entry.getName() + " is not valid from zipfile "+zipfilename);
//        	nextKeyValue();
//        }
//        boolean returnvalue = true;
//        try{
//    		System.out.println(" path name " + path.getName() + " uri " + path.toUri().toString());
//        	ZipFile zipFile = new ZipFile(path.getFileSystem(taskAttemptContext.getConfiguration()).getUri().toString());
//        	
//        }catch(Exception ex){
//        	if(returnvalue == false)
//        		return false;
//        	
//        	currentValue = new BytesWritable( ("CORRUPT_FILE:File "+ path.getName() + " is corrupt").getBytes());
//        	ex.printStackTrace();
//        	returnvalue = false;
//        	return true;
//        	
//        }
        
        if("true".equalsIgnoreCase(skipFilesonSize) && (entry.getSize() > MAX_FILE_SIZE)){
        	
        	
        	currentValue = new BytesWritable( ("FILE_SIZE_EXCEEDS_ALLOWED_LIMIT_"+MAX_FILE_SIZE).getBytes());
        	System.out.println(" Skipping file because it exceeds max file size " + entry.getName());
        	return true;
        }
        
        // Read the file contents
       
   
        while ( true )
        {
        	
            int bytesRead = 0;
            try
            {
            	Arrays.fill(temp, (byte)0);
                bytesRead = zip.read( temp, 0, 8192 );
            }
            catch(EofException e){
            	 if ( ZipFileInputFormat.getLenient() == false )
                   throw e;
            	 return false;
            }
            catch ( Exception e )
            {
            	e.printStackTrace();
//                if ( ZipFileInputFormat.getLenient() == false )
//                    throw e;
                currentValue = new BytesWritable( ("CORRUPT_FILE:File "+ path.getName() + " is corrupt").getBytes());
                return true;
//                return false;
            }
            if ( bytesRead > 0 ){
                bos.write( temp, 0, bytesRead );
               
            }
            else{

            	   break;
            }
        }
        zip.closeEntry();
        
      
       
        // Uncompressed contents
        bytesfrombos =  bos.toByteArray();
        currentValue = new BytesWritable(bytesfrombos);
      
        
        return true;
    }

/**
* Rather than calculating progress, we just keep it simple
*/
    @Override
    public float getProgress()
        throws IOException, InterruptedException
    {
        return isFinished ? 1 : 0;
    }

/**
* Returns the current key (name of the zipped file)
*/
    @Override
    public Text getCurrentKey()
        throws IOException, InterruptedException
    {
        return currentKey;
    }

/**
* Returns the current value (contents of the zipped file)
*/
    @Override
    public BytesWritable getCurrentValue()
        throws IOException, InterruptedException
    {
        return currentValue;
    }

/**
* Close quietly, ignoring any exceptions
*/
    @Override
    public void close()
        throws IOException
    {
    	if(currentValue != null){
    		currentValue.setSize(0);
   	 		currentValue = null;
    	}
   	 
    	if(bos != null){
         	bos.reset();
         	bos.close();
         	bos = null;
         }
    	 if(bytesfrombos !=null){
        	 Arrays.fill(bytesfrombos, (byte)0);
        	 bytesfrombos = null;
         }
    	 if(temp !=null){
        	 Arrays.fill(temp, (byte)0);
        	 temp = null;
         }
    	 
        try { zip.close(); } catch ( Exception ignore ) { }
        try { fsin.close(); } catch ( Exception ignore ) { }
        
       
    }
}