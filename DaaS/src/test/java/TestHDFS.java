import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;



public class TestHDFS {
	

	
	public static class STLDPathFilter implements PathFilter{

		String filter = "";
		public STLDPathFilter(){
			super();
		}
		
		public STLDPathFilter(String filter){
			
			super();
			this.filter = filter;
		}
	@Override
	public boolean accept(Path pathname) {
		
		String fileName = pathname.getName().toUpperCase();
		if(pathname.toString().contains(filter))
			return true;
		return false;
	}
	

	}
	
	public static class STLDPathFilter2 implements PathFilter{

	
	@Override
	public boolean accept(Path pathname) {
		
		String fileName = pathname.getName().toUpperCase();
		if(fileName.startsWith("STLD"))
			return true;
		return false;
	}
	

	}

	public static void main(String[] args){
		try{
		String[] inputpathstrs = args[0].split(",");
		
		String confRootHDP13 ="C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty";
		
		String confRootHDP21 = "C:/Users/mc32445/Desktop/HDP13to21Upgrade/etc/hadoop/conf";
				
		
		Configuration conf = new Configuration();
		if(args[1].equalsIgnoreCase("local")){//this will not work. network will not allow.
			
			conf.set("fs.defaultFS","hdfs://MCDHADOOPUDA");
			conf.addResource(new Path(confRootHDP21+"/core-site.xml"));
			conf.addResource(new Path(confRootHDP21+"/hdfs-site.xml"));
			conf.addResource(new Path(confRootHDP21+"/mapred-site.xml"));
			conf.addResource(new Path(confRootHDP21+"/yarn-site.xml"));
			
		}else{
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		}
		
		FileStatus[] fstatus = null;
		FileStatus[] fstatustmp = null;
		FileStatus[] fstatustmp2 = null;
		
		FileSystem hdfsFileSystem = FileSystem.get(conf);
		
	
				
		int maxFileNum = 1;
		
//		if ( fileType.length() > 0 ) {
			FileStatus[] fstats = hdfsFileSystem.globStatus(new Path("/daas/gold/np_xml/STLD/840/2014072*/STLD*"));
			if(fstats != null && fstats.length > 0){
				int fileNum = 0;
				for(int i =0;i<fstats.length;i++){
					String fileName = fstats[i].getPath().getName();
					System.out.println("fileName "+fileName);
					int extIndx = fileName.lastIndexOf(".gz");
					String fileNumVal = fileName.substring(extIndx-5,extIndx);
					
					fileNum = Integer.parseInt(fileNumVal);
					if(fileNum > maxFileNum)
						maxFileNum = fileNum;
					
				}
			}
			System.out.println("maxFileNum "+ maxFileNum);
//			new Path(fileType + HDFSUtil.FILE_PART_SEPARATOR + terrCd + HDFSUtil.FILE_PART_SEPARATOR + businessDt));;
//		}
		
		if(1 == 1 )
			return;
	
		
		
		for(String cachepathstr:inputpathstrs){
			if(cachepathstr.endsWith("*") && !cachepathstr.endsWith("STLD*")){
				int indx = cachepathstr.lastIndexOf("/");
				String filter = cachepathstr.substring(indx+1);
				filter = filter.replace("*", "");
				cachepathstr = cachepathstr.substring(0,indx);
				fstatustmp = hdfsFileSystem.listStatus(new Path(cachepathstr),new STLDPathFilter(filter));
				
				
				
				for(int i=0;i<fstatustmp.length;i++){
					FileStatus fstat = fstatustmp[i];
					fstatustmp2 = hdfsFileSystem.listStatus(fstat.getPath(),new STLDPathFilter2());
					fstatus = (FileStatus[])ArrayUtils.addAll(fstatus, fstatustmp2);
				}
				
			}else{
				fstatustmp = hdfsFileSystem.listStatus(new Path(cachepathstr),new STLDPathFilter2());
				fstatus = (FileStatus[])ArrayUtils.addAll(fstatus, fstatustmp);
			}
			

		}
		
		String filepath;
		String datepart;
		HashSet<String> dtset = new HashSet<String>();
		for(FileStatus fstat:fstatus){
			filepath = fstat.getPath().toString();
			int lastindx = filepath.lastIndexOf("~");
			if(lastindx == -1)
				lastindx = filepath.lastIndexOf("-r-");
			datepart = filepath.substring(lastindx-8,lastindx);
			dtset.add(datepart);
		}
		Iterator<String> it = dtset.iterator();
		
		while(it.hasNext()){
			datepart = it.next();
			System.out.println(" addding " +datepart);
		
		}
		
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	private static void renameMCDTMSFile(FileSystem hdfsFileSystem,String pathstr){
		try{
		FileStatus[] fstats = hdfsFileSystem.listStatus(new Path(pathstr));
		
		Path path = new Path(pathstr);
		
		String parent = path.getParent().toString();
		
		for(FileStatus fstat:fstats){
			
			String fileName = fstat.getPath().getName();
			
			String newfileName = fileName.replace("RxD126", "_")+".json";
			System.out.println(" moving file  "+ path.toString()+"/"+fileName + " to "+new Path(pathstr+"/"+newfileName).toString());
			
			if(!hdfsFileSystem.rename(new Path(path.toString()+"/"+fileName), new Path(pathstr+"/"+newfileName))){
				System.out.println(" Could not move" + path.toString()+"/"+fileName);
			}
		}
		
		
//		hdfsFileSystem.rename(arg0, arg1)
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
