package client.utils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import example.client.ContestRowMapper;

public class AllPartitionParallelClient {

	private Client client;
	private ScheduledThreadPoolExecutor m_taskExecutor;
	AtomicInteger outstandingFutures = new AtomicInteger(0);
	
	private String hostname;
	private Object[] args;
	private int batchSize;
	private int maxFrequency;
	private String procedure;

	public AllPartitionParallelClient(String hostname, int threadPoolSize) throws UnknownHostException, IOException {
		this.hostname = hostname;
		this.client = ClientFactory.createClient();
		client.createConnection(hostname);
		this.m_taskExecutor = new ScheduledThreadPoolExecutor(threadPoolSize);
	}

	public ArrayList<Object> start(String procedure, Object[] args, int batchSize, int maxFrequency) 
			throws NoConnectionsException, IOException, ProcCallException, InterruptedException, ExecutionException {
		ArrayList<Object> fullResults = new ArrayList<>();
		this.procedure = procedure;
		this.args = args;
		this.batchSize = batchSize;
		this.maxFrequency = maxFrequency;
		
		VoltTable results[] = client.callProcedure("@GetPartitionKeys", "INTEGER")
				.getResults();
		VoltTable keys = results[0];
		Future<ArrayList<Object>>[] callFutures = new Future[keys.getRowCount()];
		
		for (int k = 0;k < keys.getRowCount(); k++) {
			long partKey = keys.fetchRow(k).getLong(1);
			Future<ArrayList<Object>> callFuture  = m_taskExecutor.submit(new ClientTask(partKey));
			callFutures[k] = callFuture;
		}
		
		for(int f = 0; f<callFutures.length; f++) {
			Future<ArrayList<Object>> callFuture = callFutures[f];
			ArrayList<Object> callResults = callFuture.get();
			fullResults.addAll(callResults);
		}
		
		m_taskExecutor.shutdown();
		return fullResults;
	}
	
	private class ClientTask implements Callable<ArrayList<Object>> {
		
		SinglePartSynchronousPaginatedClient client;
		ArrayList<Object> results;
		
		public ClientTask(long partKey) throws UnknownHostException, IOException {
			client = new SinglePartSynchronousPaginatedClient(hostname, partKey, new ContestRowMapper());
			this.results = new ArrayList<Object>();
		}

		@Override
		public ArrayList<Object> call() {
			try {
				client.start(procedure, args, batchSize, maxFrequency, results);
			} catch (IOException | ProcCallException e) {
				e.printStackTrace();
				return null;
			}
			return results;
		}
	}
}
