import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


public class TestFileSplit {

	
	public static void main(String[] args){
		try{
		
		byte[] filebytes = read(new File(args[0]));
		
		
		StringBuffer sbftext = new StringBuffer(new String(filebytes));
		
		StringBuffer sbf = new StringBuffer();
		
		
		boolean foundStartTag = false;
		sbf.setLength(0);
		
		int i = 0;
		int len = sbftext.length();
		
		while(foundStartTag == false && i < len){
			
			
			
			if(sbftext.charAt(i) == '<'){
				foundStartTag = true;
			}else{
				sbf.append(sbftext.charAt(i));
			}
			i++;
		}
		
		System.out.println(sbf.toString());
		
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public static byte[] read(File file) throws IOException {



	    byte []buffer = new byte[(int) file.length()];
	    InputStream ios = null;
	    try {
	        ios = new FileInputStream(file);
	        if ( ios.read(buffer) == -1 ) {
	            throw new IOException("EOF reached while trying to read the whole file");
	        }        
	    } finally { 
	        try {
	             if ( ios != null ) 
	                  ios.close();
	        } catch ( IOException e) {
	        }
	    }

	    return buffer;
	}
}

