import javax.swing.SortingFocusTraversalPolicy;

import org.apache.commons.net.ftp.FTPClient;


public class TestFTP {

	public static void main(String[] args){
		
		try{
			FTPClient ftp;
			
		
			ftp = new FTPClient();
			
			
			ftp.connect("66.111.152.21", 22);
//			ftp.login("sateesh.pula@us.mcd.com", "jan2014m");
			
			
//			System.out.println("" + ftp.getStatus());
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
