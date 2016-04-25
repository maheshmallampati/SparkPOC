package com.mcd.gdw.test.daas.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
//import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mcd.gdw.daas.mapreduce.ZipFileInputFormat;
import com.mcd.gdw.daas.util.HDFSUtil;
import com.mcd.gdw.test.daas.mapreduce.GenericMoveMapper;
import com.mcd.gdw.test.daas.mapreduce.NextGenUnzipFileMapper;
import com.mcd.gdw.test.daas.mapreduce.NextGenUnzipFilePartitioner;
import com.mcd.gdw.test.daas.mapreduce.NextGenUnzipFileReducer;
import com.mcd.gdw.test.daas.mapreduce.NextGenUnzipFileListMapper;
import com.mcd.gdw.test.daas.mapreduce.NextGenUnzipFileListReducer;
import com.mcd.gdw.test.daas.mapreduce.NextGenUnzipFilterMapper;

public class NextGenUnzipFile  extends Configured implements Tool {

	public class ResultListFilter implements PathFilter {
		
		private String prefix = "";
		
		public ResultListFilter() {
			
		}
		
		public ResultListFilter(String prefix) {
		
			this.prefix = prefix;
			
		}
		
	    public boolean accept(Path path) {
	    	
	    	boolean retFlag = false; 
	    	
	    	if ( this.prefix.length() == 0 ) {
		    	retFlag = !path.getName().startsWith("_");
	    	} else {
	    		retFlag = path.getName().startsWith(prefix);
	    	}
	    	
	    	return(retFlag);
	    }
	}
	
	public final static String DIST_CACHE_ABAC                  = "abac.txt";
	public final static String DIST_CACHE_ABAC_IN               = "abac_in.txt";
	public final static String DIST_CACHE_REMOVE_ITEMS          = "removeitems.txt";
	public final static String CONFIG_SETTING_CTRY_ISO2_LOOKUP  = "com.mcd.gdw.daas.ctryiso2toterrcd";
	public final static String CONFIG_SETTING_VALID_TYPES       = "com.mcd.gdw.daas.validtypes";
	public final static String CONFIG_SETTING_VALID_TYPES_VALUE = "STLD|DetailedSOS|MenuItem|SecurityData|Store-Db|Product-Db";

	private HashMap<String,String> terrDtMap = new HashMap<String,String>();
	
	private int numBundles;
	private StringBuffer dtTerrParm = new StringBuffer();
	
	private FileSystem fileSystem = null;
	private Configuration hdfsConfig = null;
	private Path baseOutputPath = null;
	private Path step2Source;
	private Path step2Reject;
	private Path cache2;
	private Path cache3;
	private Path removeItemsCache;
	private Path maxFilesCache;
	private boolean existingFiles = false;
	
	//private FsPermission newFilePremission;	
	
	private Path distCacheIn;
	private Path distCache;

	
	public static void main(String[] args) throws Exception {
		
		Configuration hdfsConfig = new Configuration();
				
		int retval = ToolRunner.run(hdfsConfig,new NextGenUnzipFile(),args);

		System.out.println(" return value : " + retval);
		
	}

	
	public int run(String[] args) throws Exception {
		
		hdfsConfig = getConf();
		fileSystem = FileSystem.get(hdfsConfig);

		distCacheIn = new Path(args[1] + Path.SEPARATOR + DIST_CACHE_ABAC_IN);
		distCache = new Path(args[1] + Path.SEPARATOR + DIST_CACHE_ABAC);
		
		step2Source = new Path("/daastest/work/newunzip/np_xml/step2");
		step2Reject = new Path("/daastest/work/newunzip/np_xml/step2reject");
		
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, step2Source,true);
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, step2Source, true);
		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, step2Reject,true);
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, step2Reject, true);
		
		cache2 = new Path("/daastest/work/newunzip/cache2");
		cache3 = new Path("/daastest/work/newunzip/cache3");
		removeItemsCache = new Path(cache3.toUri() + Path.SEPARATOR + DIST_CACHE_REMOVE_ITEMS);
		maxFilesCache = new Path(cache3.toUri() + Path.SEPARATOR + "max.txt");
		
		updBndl(distCacheIn,distCache);
		 
		runJob(args[0],args[1],"/daastest/work/newunzip/np_xml/step1");	
		
		runListJob(args[0],args[2]);
		
		if ( existingFiles ) {
			runStep2MoveJob(args[2]);
			
			runFilterJob();
		} else {
			baseOutputPath = new Path("/daastest/work/newunzip/np_xml/step3");
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, baseOutputPath,true);
		}
		
		moveFilesToGold();
		
		runStep2MoveJob(args[2]);
		
		return(0);
	}

	private void moveFilesToGold() throws Exception {

		HashMap<String,Integer> maxMap = new HashMap<String,Integer>();
		String line;
		String[] parts;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new DataInputStream(fileSystem.open(maxFilesCache))));
			
		while ( (line=br.readLine()) != null) {
			parts = line.split("\t");
			maxMap.put(parts[0], Integer.parseInt(parts[1]));
		}
		
		br.close();

		ArrayList<String> fileMoveList = new ArrayList<String>();
		
		getFilesToMove("/daastest/work/newunzip/np_xml/step1",maxMap,fileMoveList);
		getFilesToMove("/daastest/work/newunzip/np_xml/step3",maxMap,fileMoveList);

		HDFSUtil.removeHdfsSubDirIfExists(fileSystem, cache2,true);
		HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, cache2, true);
		
		Path outCacheFile = null;

		int batch = 0;
		int maxFileCnt = 20;
		int fileCnt = maxFileCnt + 1;
		BufferedWriter bw = null;


		for (String itm : fileMoveList ) {
			fileCnt++;
			
			if ( fileCnt > maxFileCnt ) {
				if ( batch > 0 ) {
					bw.close();
				}
				batch++;
				outCacheFile = new Path(cache2.toUri() + Path.SEPARATOR + "movelist" + String.format("%05d",batch) + ".txt");
				bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(outCacheFile,true)));
				
				fileCnt=1;
			}
			
			bw.write(itm + "\n");
		}

		if ( batch > 0 ) {
			bw.close();
		}
		
	}
	
	private void getFilesToMove(String fileListPath
			                   ,HashMap<String,Integer> maxMap
			                   ,ArrayList<String> fileList) throws Exception {

		String[] parts;
		String[] parts2;
		int nextNum;
		Path createNew;

		FileStatus[] status = fileSystem.listStatus(new Path(fileListPath), new ResultListFilter());
		
		for ( int idx = 0; idx < status.length; idx++ ) {
			parts = status[idx].getPath().toUri().toString().split("/");
			parts2 = parts[parts.length-1].split("-r-|-m-");
			
			if ( !parts2[0].startsWith("INVALID") ) {
				parts = parts2[0].split("~");
				
				if ( maxMap.containsKey(parts2[0]) ) {
					nextNum = maxMap.get(parts2[0]) + 1;
				} else {
					nextNum = 1;
					
					createNew = new Path("/daastest/work/newunzip/gold/" + parts[0] + "/" + parts[1] + "/" + parts[2]);
					HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, createNew, true);
				}
				
				maxMap.put(parts2[0],nextNum);
				
				//System.out.println("/daastest/work/newunzip/gold/" + parts[0] + "/" + parts[1] + "/" + parts[2] + "/" + parts2[0] + "~" + String.format("%05d", nextNum) + ".gz" );
				
				fileList.add(status[idx].getPath().toUri().toString() + "\t/daastest/work/newunzip/gold/" + parts[0] + "/" + parts[1] + "/" + parts[2] + "/" + parts2[0] + "~" + String.format("%05d", nextNum) + ".gz");
				
			    //if ( fileSystem.exists(new Path("/daastest/work/newunzip/gold/" + parts[0] + "/" + parts[1] + "/" + parts[2] + "/" + parts2[0] + "~" + String.format("%05d", nextNum) + ".gz")) ) {
			    //	System.err.println("/daastest/work/newunzip/gold/" + parts[0] + "/" + parts[1] + "/" + parts[2] + "/" + parts2[0] + "~" + String.format("%05d", nextNum) + ".gz");
			    //	System.exit(8);
			    //}
			    		
			    		
			}
		}
		
	}
	
	private void runListJob(String inPath
			               ,String outPath) {

		Job job;

		try {

			baseOutputPath = new Path(outPath);
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,true);
			
			System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");

			setOutputCompression(false);

			job = Job.getInstance(hdfsConfig, "Next Gen Unzip - List files");
			
			//for (Path addPath : getVaildFilePaths("840:20150720") ) { //dtTerrParm.toString()) ) {
			for (Path addPath : getVaildFilePaths(dtTerrParm.toString()) ) {
				FileInputFormat.addInputPath(job, addPath);
				existingFiles = true;
			}

			if ( existingFiles ) {
				LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
				 
				job.addCacheFile(new URI(distCache.toString() + "#" + distCache.getName()));

				job.setJarByClass(NextGenUnzipFile.class);
				job.setMapperClass(NextGenUnzipFileListMapper.class);
				job.setNumReduceTasks(1);
				job.setReducerClass(NextGenUnzipFileListReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(NullWritable.class);
				job.setOutputKeyClass(Text.class);
				TextOutputFormat.setOutputPath(job, baseOutputPath);

				if ( ! job.waitForCompletion(true) ) {
					System.err.println("Error occured in MapReduce process, stopping");
					System.exit(8);
				}
			} else {
				HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, baseOutputPath,true);
			}

			HashMap<String,String> removeFiles = new HashMap<String,String>();
			
			FileStatus[] status = fileSystem.listStatus(baseOutputPath, new ResultListFilter("removeitems"));

			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, cache3,true);
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, cache3, true);

			if ( status != null && status.length > 0 ) {
				fileSystem.rename(status[0].getPath(), removeItemsCache);
			} else {
				OutputStream os  = fileSystem.create(removeItemsCache);
				os.close();
			}

			status = fileSystem.listStatus(baseOutputPath, new ResultListFilter("max"));

			if ( status != null && status.length > 0 ) {
				fileSystem.rename(status[0].getPath(), maxFilesCache);
			} else {
				OutputStream os  = fileSystem.create(maxFilesCache);
				os.close();
			}
			
			status = fileSystem.listStatus(baseOutputPath, new ResultListFilter("removefiles"));

			BufferedReader br = null;
			BufferedWriter bw = null;
			String line; 
			Path newDest;

			if ( status != null && status.length > 0 ) {
				br = new BufferedReader(new InputStreamReader(new DataInputStream(fileSystem.open(status[0].getPath()))));
		
				while ( (line=br.readLine()) != null) {
					if ( !removeFiles.containsKey(line) ) {
						removeFiles.put(line, "");
					}
				}
			
				br.close();
			}
			
			int batch = 0;
			int maxFileCnt = 20;
			int fileCnt = maxFileCnt + 1;
			
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, cache2,true);
			HDFSUtil.createHdfsSubDirIfNecessary(fileSystem, cache2, true);
			
			Path outCacheFile = null;
			
			status = fileSystem.listStatus(baseOutputPath, new ResultListFilter("filelist"));
			
			if ( status != null && status.length > 0 ) {
				br = new BufferedReader(new InputStreamReader(new DataInputStream(fileSystem.open(status[0].getPath()))));

				while ( (line=br.readLine()) != null) {
					fileCnt++;
					
					if ( fileCnt > maxFileCnt ) {
						if ( batch > 0 ) {
							bw.close();
						}
						batch++;
						outCacheFile = new Path(cache2.toUri() + Path.SEPARATOR + "movelist" + String.format("%05d",batch) + ".txt");
						bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(outCacheFile,true)));
						
						fileCnt=1;
					}
					
					if ( removeFiles.containsKey(line) ) {
						newDest = new Path(step2Reject.toUri() + Path.SEPARATOR + new Path(line).getName());
					} else {
						newDest = new Path(step2Source.toUri() + Path.SEPARATOR + new Path(line).getName());
					}
					
					//System.out.println(outCacheFile.getName() + "\t" + line  + "\t" + newDest.toUri());
					
					bw.write(line  + "\t" + newDest.toUri() + "\n");
				}
				
				br.close();
				if ( batch > 0 ) {
					bw.close();
				}
			}
			
			//for ( int idx=0; idx < status.length; idx++ ) {
			//	System.out.println(idx + ") " + status[idx].getPath().toUri());
			//}
			
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
		
		
	}
	
	private void runStep2MoveJob(String outPath) {

		Job job;

		try {

			baseOutputPath = new Path(outPath);
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,true);
			
			System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");

			setOutputCompression(false);

			job = Job.getInstance(hdfsConfig, "Next Gen Unzip - move files");
			
			FileInputFormat.addInputPath(job, cache2);

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

			job.setJarByClass(NextGenUnzipFile.class);
			job.setMapperClass(GenericMoveMapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( !job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
		
	}
	
	private void runFilterJob() {
		
		Job job;

		try {
			//newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

			baseOutputPath = new Path("/daastest/work/newunzip/np_xml/step3");
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,true);
			
			System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");

			setOutputCompression(true);
			
			job = Job.getInstance(hdfsConfig, "Next Gen Unzip - filter existing");
			 
			job.addCacheFile(new URI(removeItemsCache.toString() + "#" + removeItemsCache.getName()));
			
			FileInputFormat.addInputPath(job, new Path("/daastest/work/newunzip/np_xml/step2"));

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			
			job.setJarByClass(NextGenUnzipFile.class);
			job.setMapperClass(NextGenUnzipFilterMapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
	}
	
	private void runJob(String inPath
			           ,String cachePath
			           ,String outPath) {
		
		Job job;

		try {
			//newFilePremission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.READ_EXECUTE);

			baseOutputPath = new Path(outPath);
			HDFSUtil.removeHdfsSubDirIfExists(fileSystem, baseOutputPath,true);
			
			System.out.println("\nOutput path = " + baseOutputPath.toString() + "\n");

			hdfsConfig.set(CONFIG_SETTING_CTRY_ISO2_LOOKUP, "AF4  AL8  AQ10 DZ12 AS16 AD20 AO24 AG28 AZ31 AR32 AU36 AT40 BS44 BH48 BD50 AM51 BB52 BE56 BM60 BT64 BO68 BA70 BW72 BV74 BR76 BZ84 IO86 SB90 VG92 BN96 BG100MM104BI108BY112KH116CM120CA124CV132KY136CF140LK144TD148CL152CN156TW158CX162CC166CO170KM174YT175CG178CD180CK184CR188HR191CU192CY196CZ203BJ204DK208DM212DO214EC218SV222GQ226ET231ER232EE233FO234FK238GS239FJ242FI246FR250GF254PF258TF260DJ262GA266GE268GM270PS275DE276GH288GI292KI296GR300GL304GD308GP312GU316GT320GN324GY328HT332HM334VA336HN340HK344HU348IS352IN356ID360IR364IQ368IE372IL376IT380CI384JM388JP392KZ398JO400KE404KP408KR410KW414KG417LA418LB422LS426LV428LR430LY434LI438LT440LU442MO446MG450MW454MY458MV462ML466MT470MQ474MR478MU480MX484MC492MN496MD498MS500MA504MZ508OM512NA516NR520NP524NL528AN530CW531AW533NC540VU548NZ554NI558NE562NG566NU570NF574NO578MP580UM581FM583MH584PW585PK586PA591PG598PY600PE604PH608PN612PL616PT620GW624TL626PR630QA634RE638RO642RU643RW646SH654KN659AI660LC662MF663PM666VC670SM674ST678SA682SN686RS688SC690SL694SG702SK703VN704SI705SO706ZA710ZW716ES724EH732SD736SR740SJ744SZ748SE752CH756SY760TJ762TH764TG768TK772TO776TT780AE784TN788TR792TM795TC796TV798UG800UA804MK807EG818GB826TZ834US840VI850BF854UY858UZ860VE862WF876WS882YE887CS891ZM894");
			hdfsConfig.set(CONFIG_SETTING_VALID_TYPES, CONFIG_SETTING_VALID_TYPES_VALUE);

			setOutputCompression(true);
			
			job = Job.getInstance(hdfsConfig, "Next Gen Unzip");
			
			ZipFileInputFormat.setLenient(true);
			ZipFileInputFormat.setInputPaths(job,new Path(inPath));
			 
			job.addCacheFile(new URI(distCache.toString() + "#" + distCache.getName()));

			job.setInputFormatClass(ZipFileInputFormat.class);
			
			job.setJarByClass(NextGenUnzipFile.class);
			job.setMapperClass(NextGenUnzipFileMapper.class);
			job.setPartitionerClass(NextGenUnzipFilePartitioner.class);
			job.setReducerClass(NextGenUnzipFileReducer.class);
			job.setNumReduceTasks(numBundles);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			
			TextOutputFormat.setOutputPath(job, baseOutputPath);

			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);			

			if ( ! job.waitForCompletion(true) ) {
				System.err.println("Error occured in MapReduce process, stopping");
				System.exit(8);
			}

			/*
			System.out.println(job.getCounters().countCounters());
			
			Iterator<CounterGroup> iter = job.getCounters().iterator();
			while ( iter.hasNext()) {
				CounterGroup cg = iter.next();
				System.out.println(cg.getDisplayName());
				Iterator<Counter> ci = cg.iterator();
				while ( ci.hasNext() ) { 
					Counter ctr = ci.next();
					System.out.println("\t" + ctr.getDisplayName() + " = " + ctr.getValue() );
				}
			}
			*/
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

	}
	
	private void updBndl(Path in
			            ,Path out) {

		DataInputStream fileStatusStream = null;
		BufferedReader br = null;
		BufferedWriter bw = null;
		
		String[] parts; 
		
		String lastTerrCd = "X";
		String lastBusnDt = "X";
		
		int bundleSize = 125;
		int currCnt = 0;
		int bundleNum = 0;
		
		int totCnt = 0;
		
		String line;

		try {
			fileStatusStream = new DataInputStream(fileSystem.open(in));
			br = new BufferedReader(new InputStreamReader(fileStatusStream));
			
			//if ( fileSystem.exists(out) ) {
			//	fileSystem.delete(out, false);
			//}
			
			bw=new BufferedWriter(new OutputStreamWriter(fileSystem.create(out,true)));
			
			while ( (line=br.readLine()) != null) {
				
				totCnt++;
				
				parts = line.split("\t");
				
				if ( !terrDtMap.containsKey(parts[1] + ":" + parts[3]) ) {
					terrDtMap.put(parts[1] + ":" + parts[3], "");
				}
				
		        if (!parts[1].equals(lastTerrCd) ) {
		        	lastTerrCd = parts[1];
		        	lastBusnDt = parts[3];
		        	bundleNum++;
		        	currCnt = 0;
		        }
	        
		        if (!parts[3].equals(lastBusnDt) ) {
		        	lastBusnDt = parts[3];
		        	bundleNum++;
		        	currCnt = 0;
		        }
	            
		        currCnt++;
	        
		        if (currCnt > bundleSize) {
		        	bundleNum++;
		        	currCnt = 1;
		        }
		        
		        bw.write(line+"\t" + bundleNum + "\n");
				//outstream.writeChars(line+"\t" + bundleNum + "\n");
			}

			br.close();
			//outstream.close();
			bw.close();
			
			System.out.println("\nNumber of bundles = " + bundleNum);
			System.out.println("\nAverage bundle size = " +  new DecimalFormat("####0").format((double)totCnt / (double)bundleNum) + "\n\n");
			
			
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}
		
		dtTerrParm.setLength(0);
				
		for (Map.Entry<String, String> entry : terrDtMap.entrySet()) {
			//System.out.println(entry.getKey());
			
			if ( dtTerrParm.length() > 0 ) {
				dtTerrParm.append(",");
			}
			
			dtTerrParm.append(entry.getKey());
		}
		
		numBundles = bundleNum;

		//System.out.println(dtTerrParm.toString());
		
	}

	private ArrayList<Path> getVaildFilePaths(String requestedTerrDateParms) {

		String[] validTypes = CONFIG_SETTING_VALID_TYPES_VALUE.split("\\|");
		
		String[] parmItemElements;
		String terrCd;
		String frDtText;
		String toDtText;
		
		Calendar frDt = Calendar.getInstance();
		Calendar toDt = Calendar.getInstance();
		
		SimpleDateFormat fmtDate = new SimpleDateFormat("yyyyMMdd");
		
		Path listPath;
		String[] parmItems;
		//FileStatus[] fstus;
		
		ArrayList<Path> retPaths = new ArrayList<Path>();
		ArrayList<String> requestPaths = new ArrayList<String>();
		//System.out.println(" requestedTerrDateParms :" + requestedTerrDateParms);

		try {

			parmItems = requestedTerrDateParms.split(",");
		
			for ( int idx=0; idx < parmItems.length; idx++ ) {
				parmItemElements = parmItems[idx].split(":"); 
			
				terrCd = parmItemElements[0];
				frDtText = parmItemElements[1];
				if ( parmItemElements.length == 2 ) {
					toDtText = frDtText;
				} else {
					toDtText = parmItemElements[2];
				}
			
				frDt.set(Integer.parseInt(frDtText.substring(0,4)), Integer.parseInt(frDtText.substring(4,6))-1, Integer.parseInt(frDtText.substring(6,8)));
				toDt.set(Integer.parseInt(toDtText.substring(0,4)), Integer.parseInt(toDtText.substring(4,6))-1, Integer.parseInt(toDtText.substring(6,8)));
			
				while ( frDt.compareTo(toDt) <= 0 ) {
				
					requestPaths.add(terrCd + ":" + fmtDate.format(frDt.getTime()));
					frDt.add(Calendar.DAY_OF_MONTH, 1);
				}
			}
		
			for ( String terrDtItem : requestPaths ) {

				parmItemElements = terrDtItem.split(":");
				
				for ( int typeIdx=0; typeIdx < validTypes.length; typeIdx++ ) {
					//listPath = new Path("/daas/gold/np_xml" + Path.SEPARATOR + validTypes[typeIdx] + Path.SEPARATOR + parmItemElements[0] + Path.SEPARATOR + parmItemElements[1]);
					listPath = new Path("/daastest/work/newunzip/gold" + Path.SEPARATOR + validTypes[typeIdx] + Path.SEPARATOR + parmItemElements[0] + Path.SEPARATOR + parmItemElements[1]);
					
					if ( fileSystem.isDirectory(listPath) ) {
						retPaths.add(listPath);
					//} else {
					//	System.err.println(listPath.toString() + " does not exist");
					}
					
					//fstus = fileSystem.listStatus(listPath);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(8);
		}

		//for ( Path ii : retPaths ) {
		//	System.out.println(ii.toString());
		//}
		
		return(retPaths);
			
	}

	private void setOutputCompression(boolean compress) {

		if ( compress ) {
			hdfsConfig.set("mapreduce.map.output.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "true");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");
			hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
			
			System.out.println("Set output compression on");
		} else {
			hdfsConfig.set("mapreduce.map.output.compress", "false");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress", "false");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
			hdfsConfig.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");
			hdfsConfig.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");			
			
			System.out.println("Set output compression off");
		}

	}
}
