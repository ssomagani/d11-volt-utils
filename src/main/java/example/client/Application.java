package example.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.ProcCallException;

import client.utils.AllPartitionParallelClient;
import client.utils.RetryScatterCall;
import client.utils.RetryScatterCall.RetryScatterCallback;
import client.utils.SynchronousIterativeClient;

public class Application {

	static Client client;

	public static void main(String[] args) 
			throws ProcCallException, InterruptedException, ExecutionException, UnknownHostException, IOException {
		client = ClientFactory.createClient();
		client.createConnection("localhost");

		//			runRetryProc();
//					preloadData();
		//			runRepetitivePart();
		//			runRepetitiveDelete();
//		runRepetitiveSelect();
		runPartitionedKeysetPaginatedSelect();
//		runOnePartitionedKeysetPaginatedSelect();
//		runPartitionedKeysetPaginatedDelete(1);
	}
	
	private static void runOnePartitionedKeysetPaginatedSelect() 
			throws UnknownHostException, IOException, ProcCallException {
		int PARTITION_KEY = 19;
		
		Object[] args = {PARTITION_KEY, 1};
		VoltTable.ColumnInfo[] COLS = {
				new VoltTable.ColumnInfo("PARAM_1", VoltType.INTEGER), 
				new VoltTable.ColumnInfo("PARAM_2", VoltType.INTEGER)
		};
		VoltTable voltTable = new VoltTable(COLS);
		Object[] values = {0, BATCH_SIZE};
		voltTable.addRow(values);
		
		ArrayList<Object> resultRows = new ArrayList<>();
		
		SynchronousIterativeClient client = new SynchronousIterativeClient("localhost", new ContestRowMapper());
		client.start("PartitionedKeysetPaginatedSelectProc", args, voltTable, resultRows);
		
		for(Object row : resultRows) {
			System.out.println(row);
		}
		System.out.println("Returned " + resultRows.size());
	}
	
	private static void runPartitionedKeysetPaginatedDelete(int roundId) 
			throws UnknownHostException, IOException, ProcCallException, InterruptedException, ExecutionException {
		int LEAST_KEY_ID = 0;
		Object[] args = {LEAST_KEY_ID, roundId};
		AllPartitionParallelClient repetitiveClient = new AllPartitionParallelClient("localhost", 8);
		repetitiveClient.start("PartitionedIncrementalDeleteProc", args);
	}
	
	private static void runPartitionedKeysetPaginatedSelect() 
			throws UnknownHostException, IOException, ProcCallException, InterruptedException, ExecutionException {
		Object[] args = {1, 1};
		AllPartitionParallelClient repetitiveClient = new AllPartitionParallelClient("localhost", 8);
		ArrayList<Object> fullResults = repetitiveClient.start("PartitionedKeysetPaginatedSelectProc", args);
		for(Object row : fullResults) {
			System.out.println(row);
		}
		System.out.println("Returned " + fullResults.size());
	}

	static int BATCH_SIZE = 5;
	static int OFFSET = 0;

	/*private static void runRepetitiveSelect() 
			throws UnknownHostException, IOException, ProcCallException, InterruptedException, ExecutionException {
		AllPartitionParallelClient repetitiveClient = new AllPartitionParallelClient("localhost", 8);
		Object[] args = {1};
		ArrayList<Object> fullResults = repetitiveClient.start("PartitionedPaginatedSelectProc", args, BATCH_SIZE, OFFSET);
		for(Object row : fullResults) {
			System.out.println(row);
		}
		System.out.println("Returned " + fullResults.size());
	}

	private static void runRepetitiveDelete() 
			throws UnknownHostException, IOException, ProcCallException, InterruptedException, ExecutionException {
		AllPartitionParallelClient repetitiveClient = new AllPartitionParallelClient("localhost", 8);
		Object[] args = {1};
		repetitiveClient.start("LowImpactDeleteProc", args, BATCH_SIZE, OFFSET);
	}*/

	private static void deleteContest() {
		Object[] params = {1001, 5};
		try {
			client.callAllPartitionProcedure("LowImpactDeleteProc", params);
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}

	private static void runRetryProc() {
		try {
			client.createConnection("localhost");
			int maxRetryCount = 5;
			RetryScatterCall call = new RetryScatterCall(client, maxRetryCount);

			RetryScatterCallback callback = new RetryScatterCallback() {

				@Override
				public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception {

				}
			};
			call.invoke(callback, "PROCEDURE", "asdf");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void preloadData() {
		int contest_id = 1000;
		for(int i=1; i<11; i++) {
			for(int j=1; j<11; j++)  {
				for(int k=1; k<11; k++) {
					Object[] params = {contest_id++, i, 0, 10, j};
					try {
						client.callProcedure("contest_master.insert", params);
					} catch (IOException | ProcCallException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
