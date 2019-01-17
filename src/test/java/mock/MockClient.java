package mock;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.voltdb.client.AllPartitionProcedureCallback;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

import client.utils.RetryScatterCall.InternalRetryScatterCallback;
import client.utils.RetryScatterCall.RetryScatterCallback;

public class MockClient implements Client {
	
	private int tries = 1;
	private int passAt;
	
	public void setPassAt(int passAt) {
		this.passAt = passAt;
	}

	@Override
	public void createConnection(String host) throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createConnection(String host, int port) throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ClientResponse callProcedure(String procName, Object... parameters)
			throws IOException, NoConnectionsException, ProcCallException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters)
			throws IOException, NoConnectionsException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ClientResponse callProcedureWithTimeout(int queryTimeout, String procName, Object... parameters)
			throws IOException, NoConnectionsException, ProcCallException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean callProcedureWithTimeout(ProcedureCallback callback, int queryTimeout, String procName,
			Object... parameters) throws IOException, NoConnectionsException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean callProcedure(ProcedureCallback callback, int expectedSerializedSize, String procName,
			Object... parameters) throws IOException, NoConnectionsException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int calculateInvocationSerializedSize(String procName, Object... parameters) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ClientResponse updateApplicationCatalog(File catalogPath, File deploymentPath)
			throws IOException, NoConnectionsException, ProcCallException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updateApplicationCatalog(ProcedureCallback callback, File catalogPath, File deploymentPath)
			throws IOException, NoConnectionsException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ClientResponse updateClasses(File jarPath, String classesToDelete)
			throws IOException, NoConnectionsException, ProcCallException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updateClasses(ProcedureCallback callback, File jarPath, String classesToDelete)
			throws IOException, NoConnectionsException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void drain() throws NoConnectionsException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void backpressureBarrier() throws InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ClientStatsContext createStatsContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getInstanceId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getBuildString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void configureBlocking(boolean blocking) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean blocking() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int[] getThroughputAndOutstandingTxnLimits() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<InetSocketAddress> getConnectedHostList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAutoReconnectEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void writeSummaryCSV(String statsRowName, ClientStats stats, String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeSummaryCSV(ClientStats stats, String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsert,
			BulkLoaderFailureCallBack failureCallback) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize,
			BulkLoaderFailureCallBack failureCallback) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsertMode,
			BulkLoaderFailureCallBack failureCallback, BulkLoaderSuccessCallback successCallback) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClientResponseWithPartitionKey[] callAllPartitionProcedure(String procedureName, Object... params)
			throws IOException, NoConnectionsException, ProcCallException {
		MockClientResponse resp = new MockClientResponse();
		if(tries >= passAt)
			resp.status = 1;
		else
			resp.status = 0;
		
		ClientResponseWithPartitionKey respWithPartKey = new ClientResponseWithPartitionKey(1, resp);
		ClientResponseWithPartitionKey[] resps = {respWithPartKey};
		tries++;
		return resps;
	}

	public boolean callAllPartitionProcedure(InternalRetryScatterCallback callback, String procedureName,
			Object... params) throws IOException, NoConnectionsException, ProcCallException {
		MockClientResponse resp = new MockClientResponse();
		if(tries >= passAt)
			resp.status = 1;
		else
			resp.status = 0;
		
		ClientResponseWithPartitionKey respWithPartKey = new ClientResponseWithPartitionKey(1, resp);
		ClientResponseWithPartitionKey[] resps = {respWithPartKey};
		tries++;
		
		try {
			callback.clientCallback(resps);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return true;
	}

	public boolean callAllPartitionProcedure(RetryScatterCallback callback, String procedureName,
			Object... params) throws IOException, NoConnectionsException, ProcCallException {
		MockClientResponse resp = new MockClientResponse();
		if(tries >= passAt)
			resp.status = 1;
		else
			resp.status = 0;
		
		ClientResponseWithPartitionKey respWithPartKey = new ClientResponseWithPartitionKey(1, resp);
		ClientResponseWithPartitionKey[] resps = {respWithPartKey};
		try {
			callback.clientCallback(resps);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		tries++;
		
		return true;
	}

	@Override
	public boolean callAllPartitionProcedure(AllPartitionProcedureCallback callback, String procedureName,
			Object... params) throws IOException, NoConnectionsException, ProcCallException {
		try {
			InternalRetryScatterCallback internalRetryCallback = (InternalRetryScatterCallback) callback;
			return callAllPartitionProcedure(internalRetryCallback, procedureName, params);
		} catch (Exception e) {}
		try {
			RetryScatterCallback retryCallback = (RetryScatterCallback) callback;
			return callAllPartitionProcedure(retryCallback, procedureName, params);
		} catch (Exception e) {}
		return false;
	}
}
