package com.m6d.hadoopclusterprofiler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;

import com.google.gson.Gson;

public class JobClientPoller implements Runnable{

	String jobTracker;
	int pollingMS;
	int jtport;
	boolean goOn;
	private boolean firstRun=true;
	
	DataLayer dataLayer;
	
	Map<String,JobStatus> lastRunJobs; //should persist this to fs so no replay on startup
	
	public JobClientPoller(String jobTracker, int jtport, int pollingMS, DataLayer dl){
		goOn=true;
		this.pollingMS=pollingMS;
		this.jtport=jtport;
		this.dataLayer=dl;
		this.jobTracker=jobTracker;
		lastRunJobs = new HashMap<String,JobStatus>(); 
	}
	
	public void run(){
		while(goOn){
			System.out.println("running");
			long loopStart= System.currentTimeMillis();
			JobConf conf = new JobConf();
			conf.set("mapred.job.tracker",jobTracker+":"+jtport);
			JobClient jc = null;
	
			try {
				//JobStatus[] jobStatus =jc.getAllJobs();
				jc = new JobClient(conf);
				
				ClusterStatus cs = jc.getClusterStatus();
				try {
					dataLayer.persistClusterStatus(jobTracker, cs, System.currentTimeMillis());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				JobStatus[] jobStatus=jc.getAllJobs();
				Map<String,JobStatus> jobStatusMap = new HashMap<String,JobStatus>();
				for (JobStatus js : jobStatus){
					jobStatusMap.put(js.getJobId(),js);
				}
				//move through lastRunJobs
				//two types of jobs
				  //new jobs (not in lastSeen)
				  //old jobs
					//old jobs completed last cycle map & reduce hit 1
				    //old jobs that are not complete
				for (Map.Entry<String, JobStatus> entry: jobStatusMap.entrySet()){
					boolean writeStatus=false;
					boolean writeFirst=false;
					if (! this.lastRunJobs.containsKey(entry.getKey()) ){ //new job
						writeStatus=true;
						writeFirst=true;
					} else { //existed last cycle
						JobStatus jsLast = this.lastRunJobs.get(entry.getKey());
						//finished this cycle
						if ( (jsLast.getRunState() == JobStatus.SUCCEEDED) 
								|| (jsLast.getRunState() == JobStatus.FAILED)
								|| (jsLast.getRunState() == JobStatus.KILLED)){
							
						} else { //finished last cycle
							//already finished no need to write	
							writeStatus=true;
						}
					} // might be one more case of job that finishes in one cycle
								
					try {
						if (writeStatus && !firstRun){
							System.out.println("writing status");
							RunningJob runjob = jc.getJob(entry.getValue().getJobId()); 
							FileSystem fs = jc.getFs();
						
							dataLayer.persistCompletionStatus(jobTracker, entry.getValue(), System.currentTimeMillis()); 
							dataLayer.persistCompletionStatusByJobid(jobTracker, entry.getValue(), System.currentTimeMillis());
							dataLayer.persistCounters(jobTracker, runjob, System.currentTimeMillis());
							dataLayer.persistCountersByJobid(jobTracker, runjob, System.currentTimeMillis());
							if (writeFirst){
								System.out.println("writing first");
								JobConf jobConf = new JobConf(); 
							    InputStream is = fs.open(new Path(runjob.getJobFile())); //only available while job is running
							    //jobConf.addResource(new Path(runjob.getJobFile())); // wont work no error hadoop oddities.
							    jobConf.addResource( is );
								dataLayer.persistJobXML(jobTracker,jobConf,runjob, System.currentTimeMillis());//once only
								dataLayer.persistFriendlyInfo(jobTracker, jobConf, runjob, System.currentTimeMillis());//once only
								
								is.close();
							}
							
						    
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				this.lastRunJobs=jobStatusMap;
			} catch (IOException e) {
				e.printStackTrace();
			}
			long loopEnd= System.currentTimeMillis();
			long elapsed = loopEnd-loopStart;
			System.out.println("sleeping");
			if (elapsed>pollingMS){
				//error or possibly shutdown
			} 
			if (pollingMS-elapsed <=0){
				
			} else {
				try {
					Thread.sleep(pollingMS-elapsed);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			firstRun=false;
		}
	}
	

	

}
