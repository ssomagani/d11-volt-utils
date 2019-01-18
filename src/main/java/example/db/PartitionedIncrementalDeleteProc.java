package example.db;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class PartitionedIncrementalDeleteProc extends VoltProcedure {
	
	private final SQLStmt LAST_KEY_STMT = new SQLStmt("select max(contest_id) from (select contest_id from contest_master where round_id = ? and contest_id > ? order by round_id, contest_id limit ?) t");
	private final SQLStmt DELETE_STMT = new SQLStmt("delete from contest_master where round_id = ? and contest_id ? ? order by round_id, contest_id limit ?");

	public VoltTable[] run(int fakeContestId, int roundId, int lastContestId, int batchSize) {

		voltQueueSQL(LAST_KEY_STMT, roundId, lastContestId, batchSize);
		VoltTable[] lastToBeDeletedKey = voltExecuteSQL();
		
		voltQueueSQL(DELETE_STMT, roundId, lastContestId, batchSize);
		voltExecuteSQL(true);
		
		return lastToBeDeletedKey;
	}
}
