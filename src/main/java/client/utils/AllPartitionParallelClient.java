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
import org.voltdb.VoltType;
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
	private String procedure;

	public AllPartitionParallelClient(String hostname, int threadPoolSize) throws UnknownHostException, IOException {
		this.hostname = hostname;
		this.client = ClientFactory.createClient();
		client.createConnection(hostname);
		this.m_taskExecutor = new ScheduledThreadPoolExecutor(threadPoolSize);
	}

	public ArrayList<Object> start(String procedure, Object[] args) 
			throws NoConnectionsException, IOException, ProcCallException, InterruptedException, ExecutionException {
		ArrayList<Object> fullResults = new ArrayList<>();
		this.procedure = procedure;
		this.args = args;

		VoltTable results[] = client.callProcedure("@GetPartitionKeys", "INTEGER")
				.getResults();
		VoltTable keys = results[0];
		Future<ArrayList<Object>>[] callFutures = new Future[keys.getRowCount()];

		for (int k = 0;k < keys.getRowCount(); k++) {
			long partKey = keys.fetchRow(k).getLong(1);
			Future<ArrayList<Object>> callFuture  = m_taskExecutor.submit(new SynchClientTask(partKey));
			callFutures[k] = callFuture;
		}

		for(int f = 0; f<callFutures.length; f++) {
			Future<ArrayList<Object>> callFuture = callFutures[f];
			ArrayList<Object> callResults = callFuture.get();
			if(callResults != null)
				fullResults.addAll(callResults);
		}

		m_taskExecutor.shutdown();
		return fullResults;
	}

	private class SynchClientTask implements Callable<ArrayList<Object>> {

		SynchronousIterativeClient client;
		ArrayList<Object> results;
		final long partKey;
		final int BATCH_SIZE = 5;

		public SynchClientTask(long partKey) throws UnknownHostException, IOException {
			client = new SynchronousIterativeClient(hostname, new ContestRowMapper());
			results = new ArrayList<Object>();
			this.partKey = partKey;
			System.out.println("partkey = " + partKey);
		}

		@Override
		public ArrayList<Object> call() throws Exception {
			VoltTable.ColumnInfo[] COLS = {
					new VoltTable.ColumnInfo("BATCH_SIZE", VoltType.INTEGER), 
					new VoltTable.ColumnInfo("KEY_1", VoltType.INTEGER)
			};
			VoltTable voltTable = new VoltTable(COLS);
			Object[] values = {BATCH_SIZE, 0};
			voltTable.addRow(values);
			Object[] finalArgs = appendToArray(partKey, args);
			try {
				client.start(procedure, finalArgs, voltTable, results);
			} catch (IOException | ProcCallException e) {
				e.printStackTrace();
				return null;
			}
			return results;
		}
	}

	private Object[] appendToArray(long partKey, Object[] args) {
		Object[] finalArgs = new Object[args.length + 1];

		finalArgs[0] = partKey;

		for(int i=0; i<args.length; i++) {
			finalArgs[i+1] = args[i];
		}

		return finalArgs;
	}
}
