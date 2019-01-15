package mock;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

public class MockClientResponse implements ClientResponse {

	byte status;
	
	public void setStatus(byte status) {
		this.status = status;
	}
	
	@Override
	public byte getStatus() {
		return status;
	}

	@Override
	public byte getAppStatus() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public VoltTable[] getResults() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStatusString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAppStatusString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getClusterRoundtrip() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getClientRoundtrip() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getClientRoundtripNanos() {
		// TODO Auto-generated method stub
		return 0;
	}

}
