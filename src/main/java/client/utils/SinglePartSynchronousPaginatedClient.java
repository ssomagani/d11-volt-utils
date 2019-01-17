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

public class SinglePartSynchronousPaginatedClient {

	private final Client client;
	private final long partKey;
	private final Function<VoltTable, ArrayList<Object>> rowMapper;
	
	public SinglePartSynchronousPaginatedClient(String hostname, long partKey, Function<VoltTable, ArrayList<Object>> rowMapper) 
			throws UnknownHostException, IOException {
		this.client = ClientFactory.createClient();
		client.createConnection(hostname);
		this.partKey = partKey;
		this.rowMapper = rowMapper;
	}
	
	public void start(String procName, Object[] args, int batchSize, int offset, ArrayList<Object> resultRows) 
			throws NoConnectionsException, IOException, ProcCallException {
		
		Object[] finalArgs = appendToArray(args, batchSize, offset);
		ClientResponse response = client.callProcedure(procName, finalArgs);

		if(response.getResults().length > 0) {
			VoltTable statsTable = response.getResults()[0];
			
			if(response.getResults().length > 1) {
				VoltTable rowsTable = response.getResults()[1];
				resultRows.addAll(rowMapper.apply(rowsTable));
			}
			
			if(statsTable.advanceRow()) {
				int rowsLeft = (int) statsTable.getLong(0);
				System.out.println(rowsLeft + " : " + statsTable.getLong(1));
				if(rowsLeft > 0) {
					start(procName, args, batchSize, offset+batchSize, resultRows);
				}
			}
		}
	}
	
	private Object[] appendToArray(Object[] args, int batchSize, int offset) {
		Object[] finalArgs = new Object[args.length + 3];
		
		finalArgs[0] = partKey;
		
		if(args.length == 0) {
			finalArgs[1] = batchSize;
		}
		for(int i=0; i<args.length; i++) {
			finalArgs[i+1] = args[i];
		}
		finalArgs[args.length+1] = batchSize;
		finalArgs[args.length+2] = offset;
		
//		System.out.println("finalArgs = " + finalArgs[0] + " " + finalArgs[1] + " " + finalArgs[2] + " " + finalArgs[3]);
		
		return finalArgs;
	}
}
