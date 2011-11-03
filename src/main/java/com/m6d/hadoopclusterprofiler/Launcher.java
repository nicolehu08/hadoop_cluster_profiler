package com.m6d.hadoopclusterprofiler;

import java.util.HashMap;
import java.util.Map;

public class Launcher {
	public static void main(String[] args) {
		Map <String,String>conf = new HashMap<String,String>();
		for (String arg : args) {
			if (arg.contains("=")) {
				String vname = arg.substring(0, arg.indexOf('='));
				String vval = arg.substring(arg.indexOf('=') + 1);
				conf.put(vname, vval.replace("\"", ""));
				System.out.println(vname);
				System.out.println(vval);
			}
		}
		DataLayer dataLayer=null;
		try {
			dataLayer = new DataLayer(conf.get(Conf.cassandra_hosts),
					Integer.parseInt(conf.get(Conf.cassandra_port))
			);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		JobClientPoller p = new JobClientPoller(conf.get(Conf.jt_host), 
				Integer.parseInt(conf.get(Conf.jt_port)), 
				Integer.parseInt(conf.get(Conf.delay_ms)),
				dataLayer);
		
		Thread pollerThread = new Thread(p);
		pollerThread.start();
	}
}
