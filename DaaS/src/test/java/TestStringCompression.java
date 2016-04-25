import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;


public class TestStringCompression {
	public static void main(String[] args){
		
		try{
		TestStringCompression testTest2 = new TestStringCompression();
		
		String compStr =  testTest2.compressString(args[0] );
		
		System.out.println( " compressed " +compStr + " len " + compStr.length());
		
		System.out.println( " uncompressed " + testTest2.decompressString(compStr ));
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

	


	public  String compressString(String str) {
		ByteArrayOutputStream out = null;
		try{
			
			if (str == null || str.length() == 0) {
			    return str;
			}
			out = new ByteArrayOutputStream(str.length());
			GZIPOutputStream gzip = new GZIPOutputStream(out);
			gzip.write(str.getBytes());
			gzip.close();
			out.close();
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return Base64.encodeBase64String(out.toByteArray()).replaceAll("[\n\r]", "").replaceAll("\t", "");
	}
	
	public String decompressString(String str) throws IOException{
		StringBuffer newsbf = new StringBuffer();
		
		byte[] bytes = Base64.decodeBase64(str);
		GZIPInputStream gzis = null;
		try{
		    gzis = new GZIPInputStream(new ByteArrayInputStream(bytes));
		    
		   
		}catch(Exception ex){
			ex.printStackTrace();
		}
	    
	    return IOUtils.toString(gzis);
	}

}