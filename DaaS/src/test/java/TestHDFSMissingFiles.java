import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TestHDFSMissingFiles {

	public static void main(String[] args){
		try{
		String[] inputpathstrs = args[0].split(",");
		
		Configuration conf = new Configuration();
		if(args[0].equalsIgnoreCase("local")){//this will not work. network will not allow.
			conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/core-site.xml"));
			conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/hdfs-site.xml"));
			conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/mapred-site.xml"));
		}else{
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		}
		FileStatus[] fstatus = null;
		FileStatus[] fstatustmp1 = null;
		FileStatus[] fstatustmp2 = null;
		
		
		
		FileSystem hdfsFileSystem = FileSystem.get(conf);
		
		fstatustmp1 = hdfsFileSystem.listStatus(new Path("/daas/lz/abacbackup20140519"));
		fstatustmp2 = hdfsFileSystem.listStatus(new Path("/daas/lz/abac"));
		
		HashSet<String> filelist1 = new HashSet<String>();
		HashSet<String> filelist2 = new HashSet<String>();
		
		for(int indx1=0;indx1 < fstatustmp1.length;indx1++){
			filelist1.add(fstatustmp1[indx1].getPath().getName());
		}
		
		for(int indx2=0;indx2 < fstatustmp2.length;indx2++){
			filelist2.add(fstatustmp1[indx2].getPath().getName());
		}
		
		
		Iterator<String> it = filelist1.iterator();
		
		int count = 0;
		while(it.hasNext()){
			String fname = it.next();
			if(!filelist2.contains(fname)){
				count++;
				System.out.println(fname);
			}
		}
		
		System.out.println(" missing file count " + count);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
