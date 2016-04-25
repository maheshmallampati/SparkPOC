
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.io.BytesWritable;



public class TestZipfileReading

{

	public static void main2(String[] args){
//		StringBuffer sbf = new StringBuffer();
//		
//		sbf.append("Sateesh").append(" ");
//		
//		System.out.println(" sbf length " + sbf.length());
//		
//		System.out.println(" sbd string length " + sbf.toString().length());
//	
		String key = args[0];
		
		System.out.println("key.hashCode() " + key.hashCode());
		System.out.println("key.hashCode() & Integer.MAX_VALUE " + (key.hashCode() & Integer.MAX_VALUE));
		System.out.println((key.hashCode() & Integer.MAX_VALUE) % 40);
	}
	
	// Expands the zip file passed as argument 1, into the
    // directory provided in argument 2
	public static void main(String[] args){
     try{
        if(args.length != 2)
        {
            System.err.println("zipreader zipfile outputdir");
            return;
        }

       
        System.out.println( " calling unzip ");
        
        readzipxusingzip(args[0],args[1]);
        
//        readzipxusingapache(args[0]);
        
//        readzipxusinglzma(args[0]);
    }catch(Exception ex){
    	ex.printStackTrace();
    }
    }
    
    private static void readzipxusingzip(String zipfile,String outdir){
    	 ZipInputStream stream = null;
    	 try
          {
    		 System.out.println( " readzipxusingzip called ");
    		 
    		 try{
    	        	ZipFile zipFile = new ZipFile(zipfile);
    	        	
    	        }catch(Exception ex){
    	        
    	        	
    	      
    	        	ex.printStackTrace();
    	        	
    	        	
    	        }
    		 
    		  // create a buffer to improve copy performance later.
    	        byte[] buffer = new byte[2048];
    	     
    	        ZipFile zf = new ZipFile(new File(zipfile));
    	        
    	     // open the zip file stream
    		  InputStream theFile = new FileInputStream(zipfile);
    		  stream = new ZipInputStream(theFile);
              // now iterate through each item in the stream. The get next
              // entry call will return a ZipEntry for each file in the
              // stream
              ZipEntry entry;
              System.out.println(" before while ");
              while((entry = stream.getNextEntry())!=null)
              {
            	  System.out.println("entered while");
//            	  entry.setMethod(14);
                  String s = String.format("Entry: %s len %d added %TD",
                                  entry.getName(), entry.getSize(),
                                  new Date(entry.getTime()));
                  System.out.println(s);

                  // Once we get the entry from the stream, the stream is
                  // positioned read to read the raw data, and we keep
                  // reading until read returns 0 or less.
                  String outpath = outdir + "/" + entry.getName();
                  FileOutputStream output = null;
                  try
                  {
                      output = new FileOutputStream(outpath);
                      int len = 0;
                      while ((len = stream.read(buffer)) > 0)
                      {
                          output.write(buffer, 0, len);
                      }
                  }
                  finally
                  {
                      // we must always close the output file
                      if(output!=null) output.close();
                  }
              }
          }catch(Exception ex){
        	  ex.printStackTrace();
          }
          finally
          {
        	  try{
              // we must always close the zip file.
        		  if(stream != null)
        			  stream.close();
        	  }catch(Exception ex){
        		  ex.printStackTrace();
        	  }
          }
    }
//    private static void readzipxusinglzma(String zipfile){
//    	LzmaInputStream  lzmais = null;
//    	try{
//    	
//    		ZipFile zipFile = new ZipFile(zipfile);
//    		
//    		
//    		Enumeration<?> entries = zipFile.entries();
//    		
//    		ZipEntry zipentry = (ZipEntry)entries.nextElement();
//    		
//    		
//    		
////    		FileInputStream inFile = new FileInputStream(zipfile);
//            lzmais = new LzmaInputStream(zipFile.getInputStream(zipentry));
//            
//            BufferedReader bfr = new BufferedReader(new InputStreamReader(lzmais));
//            byte[] buf = new byte[65536];
//            String line;
//            int n;
//            while ((n =lzmais.read(buf)) != -1) {
//              if (n > 0) {
//                  System.out.println(new String(buf));
//              }
//          }
//       
//    	}catch(Exception ex){
//    		ex.printStackTrace();
//    	}finally{
//    		try{
//    		if(lzmais != null)
//    			lzmais.close();
//    		}catch(Exception ex){
//    			ex.printStackTrace();
//    		}
//    	}
//    }
    
    private static void readzipxusingapache(String zipfile){
//    	try{
//    	ZipFile zipFile = new ZipFile(zipfile);
//    	
//        byte[] buf = new byte[65536];
//        Enumeration<?> entries = zipFile.getEntries();
//        while (entries.hasMoreElements()) {
//            ZipArchiveEntry zipArchiveEntry = (ZipArchiveEntry) entries.nextElement();
//            int n;
//            InputStream is = zipFile.getInputStream(zipArchiveEntry);
//            ZipArchiveInputStream zis = new ZipArchiveInputStream(is);
//            if (zis.canReadEntryData(zipArchiveEntry)) {
//                while ((n = is.read(buf)) != -1) {
//                    if (n > 0) {
//                        System.out.println(new String(buf));
//                    }
//                }
//            }
//            zis.close();
//        }
//    	}catch(Exception ex){
//    		ex.printStackTrace();
//    	}finally{
//    		
//    	}
    }
}