import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class TestMoveFilestoHDFS {

		public static void main(String[] args){
			try{
			Configuration conf = new Configuration();
			conf.set("fs.default.name", "hdfs://hdp001-nn:8020");
			conf.set("user.name","psinkula");
			if(args[1].equalsIgnoreCase("local")){//this will not work. network will not allow.
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/core-site.xml"));
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/hdfs-site.xml"));
				conf.addResource(new Path("C:/Users/mc32445/Desktop/DaaS/newclusterconf/conf.empty/mapred-site.xml"));
			}else{
				conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
			}
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/daastest/oozieworkflow/workflow.xml");
			
			Path newpath = new Path( path.getParent(),"lib");
			System.out.println( " path parent " + newpath.toUri());

//			fs.copyFromLocalFile(new Path(args[0]), new Path("/daastest/oozieclienttest/oozieworkflow/lib/daasmapreduce.jar"));
			
			File folder = new File(args[0]);
			File[] listOfFiles = folder.listFiles();
			 for (int i = 0; i < listOfFiles.length; i++) {
			      if (listOfFiles[i].isFile()) {
			    	  String fname = listOfFiles[i].getName();
			    	  int indx = fname.indexOf(".");
			    	  if(indx < 0)
			    		  System.out.println( " fname " + fname);
			    	  if(indx > 0 && !fs.exists(new Path(args[2]+"/"+fname.substring(0,indx)))){
			    		  fs.copyFromLocalFile(new Path(args[0]+"/"+fname), new Path(args[2]+"/"+fname.substring(0,indx)));
			    	  }
			      }
			 }

			
			
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
}
