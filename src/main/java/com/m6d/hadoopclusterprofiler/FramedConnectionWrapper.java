package com.m6d.hadoopclusterprofiler;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

public class FramedConnectionWrapper {

	private TTransport transport;
	private TProtocol proto;
	private TSocket socket;

	public FramedConnectionWrapper(String host, int port) {
		socket = new TSocket(host, port);
		transport = new TFramedTransport(socket);
		proto = new TBinaryProtocol(transport);
	}

	public void open() throws Exception {
		transport.open();
	}

	public void close() throws Exception {
		transport.close();
		socket.close();
	}

	public Cassandra.Client getClient() throws Exception {
		if (transport==null){ 
			open();
		}
		if (!transport.isOpen())
			open();
		Cassandra.Client client = new Cassandra.Client(proto);
		return client;
	}
}
