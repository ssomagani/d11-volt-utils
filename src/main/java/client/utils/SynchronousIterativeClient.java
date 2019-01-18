package client.utils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.function.Function;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class SynchronousIterativeClient {

	private final Client client;
	private final Function<VoltTable, ArrayList<Object>> rowMapper;
	
	public SynchronousIterativeClient(String hostname, Function<VoltTable, ArrayList<Object>> rowMapper) 
			throws UnknownHostException, IOException {
		this.client = ClientFactory.createClient();
		client.createConnection(hostname);
		this.rowMapper = rowMapper;
	}
	
	public void start(String procName, Object[] args, VoltTable paginationParams, ArrayList<Object> resultRows) 
			throws NoConnectionsException, IOException, ProcCallException {
		ClientResponse response = client.callProcedure(procName, args[0], args[1], paginationParams);
		if(response.getResults().length > 0) {
			VoltTable rowsTable = response.getResults()[0];
			int rowsRetrieved = rowsTable.getRowCount();
			
			if(rowsRetrieved > 0) {
				resultRows.addAll(rowMapper.apply(rowsTable));
				long lastKey = (long) resultRows.get(resultRows.size() - 1);
				updateVoltTable(paginationParams, lastKey);
				start(procName, args, paginationParams, resultRows);
			}
		}
	}
	
	private void updateVoltTable(VoltTable paginationParams, long lastFetchedKey) {
		paginationParams.resetRowPosition();
		if(paginationParams.advanceRow()) {
			int batchSize = (int) paginationParams.getLong("BATCH_SIZE");
			paginationParams.clearRowData();
			Object[] args = {lastFetchedKey, batchSize};
			paginationParams.addRow(args);
		}
	}
}
