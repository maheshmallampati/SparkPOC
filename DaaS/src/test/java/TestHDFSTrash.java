import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TestHDFSTrash {

	public static  void main(String[] args){
		
		try{
			Configuration conf = new Configuration();
			conf.addResource("/etc/hadoop/conf/core-site.xml");
			conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
			conf.addResource("/etc/hadoop/conf/mapred-site.xml");
			
			FileSystem fs = FileSystem.get(conf);
			String inputRootDir = args[0];
			
			String[] inputpathstrs = inputRootDir.split(",");
			FileStatus[] fstatus = null;
			FileStatus[] fstatustmp = null;
			
			for(String cachepathstr:inputpathstrs){
				System.out.println( " cachepathstr " + cachepathstr);
				fstatustmp = fs.listStatus(new Path("/daastest/testtdaanddaypartextracts/stldextract/stld/"));
//				fstatustmp = fs.listStatus(new Path(cachepathstr), new PathFilter() {
//					
//					@Override
//					public boolean accept(Path pathname) {
//						System.out.println(" pathname.getName() " + pathname.getName());
//						if(pathname.getName().startsWith("PMIX"))
//							return true;
//								
//						return false;
//					}
//				});
				
				System.out.println( " fstatustmp.length " +fstatustmp.length);
				
				for(FileStatus fstattmp:fstatustmp){
					System.out.println( " fstattmp " +fstattmp.getPath().toString() );
				}
				fstatus = (FileStatus[])ArrayUtils.addAll(fstatus, fstatustmp);
			}
			String filepath;
			String datepart;
			HashSet<String> dtset = new HashSet<String>();
			for(FileStatus fstat:fstatus){
				filepath = fstat.getPath().getName();
				int lastindx = filepath.indexOf("-");
				datepart = filepath.substring(4,lastindx);
				dtset.add(datepart);
			}
			Iterator<String> it = dtset.iterator();
			
			while(it.hasNext()){
				datepart = it.next();
				System.out.println(" trying to add " +datepart);
				
			}
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
