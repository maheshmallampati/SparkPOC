import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;


public class TestHadoopPartition {

	
	public static void main(String[] args){
		BufferedReader br = null;
		try{
			InputStreamReader is = new InputStreamReader(new FileInputStream(new File("C:/Users/mc32445/Desktop/DaaS/dates")));
			br = new BufferedReader(is);
			String key ;
			String tmp;
			while(( key = br.readLine() ) != null){
				tmp = key;
				key = tmp.substring(0,4)+"-"+tmp.substring(4,6)+"-"+tmp.substring(6);
//				System.out.println("key.hashCode() " + key.hashCode());
				System.out.println(key + "|"+ (key.hashCode() & Integer.MAX_VALUE)%55);
//				System.out.println((key + " - " +(key.hashCode() & Integer.MAX_VALUE) % 55));
//				System.out.println( key + " - " + (Math.abs(key.hashCode()*8)%55));
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
