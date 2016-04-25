package com.mcd.gdw.test.daas.mapreduce;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @source https://github.com/rjpower/hadoop-zip/blob/master/org/apache/hadoop/mapred/ZipFileOutputFormat.java
 *
 * @param <K>
 * @param <V>
 */
public class ZipFileOutputFormat<K, V> extends FileOutputFormat<K, V> {
	
  public static class ZipRecordWriter<K, V> extends
      org.apache.hadoop.mapreduce.RecordWriter<K, V> {
    private ZipOutputStream zipOut;

    public ZipRecordWriter(FSDataOutputStream fileOut) {
      zipOut = new ZipOutputStream(fileOut);
    }

    @Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
    	 zipOut.finish();
	      zipOut.close();
		
	}

    
	@Override
    public void write(K key, V value) throws IOException {
      String fname = null;
      if (key instanceof BytesWritable) {
        BytesWritable bk = (BytesWritable)key;
        fname = new String(bk.getBytes(), 0, bk.getLength());
      } else {
        fname = key.toString();
      }

      ZipEntry ze = new ZipEntry(fname);
      zipOut.closeEntry();
      zipOut.putNextEntry(ze);

      if (value instanceof BytesWritable) {
        zipOut.write(((BytesWritable) value).getBytes(), 0, ((BytesWritable)value).getLength());
      } else {
        zipOut.write(value.toString().getBytes());
      }

    }

  }
//
//  @Override
//  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
//      String name, Progressable progress) throws IOException {
//    Path file = FileOutputFormat.getTaskOutputPath(job, name);
//    FileSystem fs = file.getFileSystem(job);
//    FSDataOutputStream fileOut = fs.create(file, progress);
//    return new ZipRecordWriter<K, V>(fileOut);
//  }
  
  

@Override
public org.apache.hadoop.mapreduce.RecordWriter<K, V> getRecordWriter(
		TaskAttemptContext job) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	Configuration conf = job.getConfiguration();
	 FileOutputCommitter committer = 
		      (FileOutputCommitter) getOutputCommitter(job);
	 
	String fileName = getOutputName(job);
	
//	Path file = getDefaultWorkFile(job,".zip");
	Path file =  new Path(committer.getWorkPath()+"/"+fileName);
	
	 FileSystem fs = file.getFileSystem(conf);
	FSDataOutputStream fileOut = fs.create(file);
	
	 
	return new ZipRecordWriter(fileOut);
	
}
}
