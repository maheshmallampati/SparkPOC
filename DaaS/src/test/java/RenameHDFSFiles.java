import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class RenameHDFSFiles {

	
	public static void main(String[] args){
		
		try{
			String[] inputpathstrs = args[0].split(",");
			
			Configuration conf = new Configuration();
			if(args[1].equalsIgnoreCase("local")){//this will not work. network will not allow.
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/core-site.xml"));
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/hdfs-site.xml"));
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/mapred-site.xml"));
			}else{
				conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
			}
			FileSystem hdfsFileSystem = FileSystem.get(conf);
			
			FileStatus[] fstatus = hdfsFileSystem.listStatus(new Path(args[0]));
			
			for(FileStatus fstat:fstatus){
				
				String fileName = fstat.getPath().getName();
				if(!fileName.startsWith("MCD"))
					continue;
				Path filePath = fstat.getPath();
				int lastIndx = filePath.toString().lastIndexOf("/");
				
				String fileRoot = filePath.toString().substring(0,lastIndx);
				String newFileName = fileName.replace("RxD126", "_")+".json";
				
				hdfsFileSystem.rename(filePath, new Path(fileRoot+"/"+newFileName));
				
				System.out.println(fileRoot+"/"+newFileName);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
			
	}
}
