package com.m6d.hadoopclusterprofiler;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.thrift.TException;

import com.google.gson.Gson;

public class DataLayer {

	String cassandraHosts;
	String keyspace="hadoop_cluster_profiler";
	FramedConnectionWrapper wrap;
	SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
	
	public DataLayer(String cassandraHosts, int cassandraPort) throws Exception{
		this.cassandraHosts=cassandraHosts;
		wrap = new FramedConnectionWrapper(cassandraHosts,cassandraPort);
		wrap.open();
	}
	
	public String getFormattedDay(long time){
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTimeInMillis(time);
		return sf.format( gc.getTime() );
	}
	
	public void persistFriendlyInfo(String jobTracker, JobConf jobConf, RunningJob runjob, long time) throws Exception{
		String cf="friendly_to_job";
		String trackingName=jobConf.get("tracking.name");
		if (trackingName==null){
			System.out.println("no tracking.name");
			return;
		}
		
		//Gson gson = new Gson();
		//String json = gson.toJson(persist);
		String key= jobTracker+"/"+getFormattedDay(time)+"/"+trackingName;
		
		Column c = new Column();
		c.setName( ByteBufferUtil.bytes(runjob.getJobID()) );
		c.setValue( ByteBufferUtil.bytes("") );
		c.setTimestamp(time*1000L);
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
	    
		try {
			wrap.getClient().set_keyspace(keyspace);
			wrap.getClient().insert(ByteBufferUtil.bytes(key), cp, c, ConsistencyLevel.ONE);
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
		
		
	}
	
	public void persistJobXML(String jobTracker,JobConf jobConf, RunningJob runjob, long time) throws Exception {
		String cf="jobxml";
	    Map<String,String> persist = new HashMap<String,String>();
	    Iterator <Entry<String,String>> iterator =jobConf.iterator();
	    while (iterator.hasNext()){
	    	Entry<String,String> entry = iterator.next();
	    	persist.put(entry.getKey(), entry.getValue());
	    }
	 
		Gson gson = new Gson();
		String json = gson.toJson(persist);
		String key= jobTracker+"/"+runjob.getJobID();
		
		Column c = new Column();
		c.setName( ByteBufferUtil.bytes("xmlfile") );
		c.setValue( ByteBufferUtil.bytes(json) );
		c.setTimestamp(time*1000L);
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
	    
		try {
			wrap.getClient().set_keyspace(keyspace);
			wrap.getClient().insert(ByteBufferUtil.bytes(key), cp, c, ConsistencyLevel.ONE);
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
	    
	}
	// "jt/2011-01-01" "LONG" {job_id, counter[]}  
	public void persistCounters(String jobTracker, RunningJob runjob, long time) throws Exception {
		String cf="jobcounters";
		SimpleRunningJob srj = new SimpleRunningJob(runjob);
		Gson gson = new Gson();
		String json = gson.toJson(srj);
		String key= jobTracker+"/"+getFormattedDay(time);
		
		Column c = new Column();
		c.setName( longToByteArray(time) );
		c.setValue( ByteBufferUtil.bytes(json) );
		c.setTimestamp(time*1000L);
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
		
		try {
			wrap.getClient().set_keyspace(keyspace);
			wrap.getClient().insert(ByteBufferUtil.bytes(key), cp, c, ConsistencyLevel.ONE);
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
	}
	
	public void persistCountersByJobid(String jobTracker, RunningJob runjob, long time) throws Exception {
		//create column family jobcountersbyjobid with comparator=LongType and default_validation_class=UTF8Type;
		String cf="jobcountersbyjobid";
		SimpleRunningJob srj = new SimpleRunningJob(runjob);
		Gson gson = new Gson();
		String json = gson.toJson(srj);
		String key= jobTracker+"/"+srj.job_id;
		
		Column c = new Column();
		c.setName( longToByteArray(time) );
		c.setValue( ByteBufferUtil.bytes(json) );
		c.setTimestamp(time*1000L);
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
		
		try {
			wrap.getClient().set_keyspace(keyspace);
			wrap.getClient().insert(ByteBufferUtil.bytes(key), cp, c, ConsistencyLevel.ONE);
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
	}
	
	
	
	
	
	public void persistCompletionStatusByJobid(String jobTracker, JobStatus js, long time) throws Exception {
		//create column family jobcompletionstatusbyjobid with comparator=LongType and default_validation_class=UTF8Type;
		String cf="jobcompletionstatusbyjobid";
		SimpleJobStatus sjs = new SimpleJobStatus(js);
		Gson gson = new Gson();
		String json = gson.toJson(sjs);
		String key= jobTracker+"/"+sjs.jobid;
		
		
		Column c = new Column();
		c.setName( longToByteArray(time) );
		c.setValue( ByteBufferUtil.bytes(json) );
		c.setTimestamp(time*1000L);
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
		
		try {
			wrap.getClient().set_keyspace(keyspace);
			wrap.getClient().insert(ByteBufferUtil.bytes(key), cp, c, ConsistencyLevel.ONE);
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
	}
	
	public void persistCompletionStatus(String jobTracker, JobStatus js, long time) throws Exception {
		String cf="jobcompletionstatus";
		SimpleJobStatus sjs = new SimpleJobStatus(js);
		Gson gson = new Gson();
		String json = gson.toJson(sjs);
		String key= jobTracker+"/"+getFormattedDay(time);
		
		Column c = new Column();
		c.setName( longToByteArray(time) );
		c.setValue( ByteBufferUtil.bytes(json) );
		c.setTimestamp(time*1000L);
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
		
		try {
			wrap.getClient().set_keyspace(keyspace);
			wrap.getClient().insert(ByteBufferUtil.bytes(key), cp, c, ConsistencyLevel.ONE);
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
		
	} 
	
	//start and end should be the same day here
	public List<String> statusForTime
	(String jobTracker, GregorianCalendar start, GregorianCalendar end) throws Exception{
		String key= jobTracker+"/"+getFormattedDay(start.getTime().getTime());
		List<String> results = new ArrayList<String>();
		String cf="clusterstatus";
		//Gson gson = new Gson();
		//String json = new String();
		//SimpleClusterStatus scs = gson.fromJson(json, SimpleClusterStatus.class);
		
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sr = new SliceRange();
		sr.setStart(longToByteArray(start.getTimeInMillis()));
		sr.setFinish(longToByteArray(start.getTimeInMillis()));
		sr.setCount(30);
		predicate.setSlice_range(sr);
		try {
			wrap.getClient().set_keyspace(keyspace);
			List<ColumnOrSuperColumn> cols =wrap.getClient().get_slice(ByteBufferUtil.bytes(key), cp, predicate, ConsistencyLevel.ONE);
			for (ColumnOrSuperColumn c:cols){
				results.add(ByteBufferUtil.string(c.column.value));
			}
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
		return results;
	}
	
	public void persistClusterStatus(String jobTracker, ClusterStatus cs, long time) throws Exception {	
		String cf="clusterstatus";
		SimpleClusterStatus scs = new SimpleClusterStatus(cs); 
		Gson gson = new Gson();
		String json = gson.toJson(scs);
		String key= jobTracker+"/"+getFormattedDay(time);
		Column c = new Column();
	
		c.setName( longToByteArray(time) );
		c.setValue( ByteBufferUtil.bytes(json) );
		c.setTimestamp(time*1000L);
		ColumnParent cp = new ColumnParent();
		cp.setColumn_family(cf);
		try {
			wrap.getClient().set_keyspace(keyspace);
			wrap.getClient().insert(ByteBufferUtil.bytes(key), cp, c, ConsistencyLevel.ONE);
		} catch (InvalidRequestException e) {
			throw e;
		} catch (TException e) {
			throw e;
		} catch (UnavailableException e) {
			throw e;
		} catch (TimedOutException e) {
			throw e;
		}
	}
	
	public void finalize(){
		try {
			wrap.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public byte[] longToByteArray(long l) throws Exception{
		 ByteArrayOutputStream bos = new ByteArrayOutputStream();
	      DataOutputStream dos = new DataOutputStream(bos);
	      dos.writeLong(l);
	      dos.flush();
	      return bos.toByteArray() ;
	}
}

class SimpleClusterStatus{
	Integer mapTasks;
	Integer reduceTasks;
	Integer maxMapTasks;
	Integer maxReduceTasks;
	
	public SimpleClusterStatus(){
		
	}
	
	public SimpleClusterStatus(ClusterStatus cs){
		this.mapTasks=cs.getMapTasks();
		this.maxMapTasks=cs.getMaxMapTasks();
		this.reduceTasks=cs.getReduceTasks();
		this.maxReduceTasks=cs.getMaxReduceTasks();
	}
}

class SimpleJobStatus{
	String jobid;
	Float map_completion_percent;
	Float reduce_completion_percent;
	
	public SimpleJobStatus(){}
	
	public SimpleJobStatus(JobStatus js){
		this.jobid=js.getJobId();
		this.map_completion_percent=js.mapProgress();
		this.reduce_completion_percent=js.reduceProgress();
	}
}

class SimpleRunningJob{
	String job_id;
	List<SimpleCounter> counters = new ArrayList<SimpleCounter>();
	public SimpleRunningJob(RunningJob jb){
		job_id = jb.getJobID();
		try {
			Counters c = jb.getCounters();
			for (String group: c.getGroupNames()){
				Group g = c.getGroup(group);
				Iterator<Counter> i = g.iterator();
				while (i.hasNext()){
					Counter count = i.next();
					SimpleCounter sc = new SimpleCounter(g.getName(),count.getName(),count.getValue());
					counters.add(sc);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class SimpleCounter{
	
	String group;
	String name;
	Long value;
	
	public SimpleCounter(){}
	
	public SimpleCounter(String group,String name, Long value){
		this.group=group;
		this.name=name;
		this.value=value;
	}
}
