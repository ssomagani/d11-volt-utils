package example.db;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class LowImpactDeleteProc extends VoltProcedure {

	private final SQLStmt SELECT_STMT = new SQLStmt("select count(*) from contest_master where round_id = ?");
	private final SQLStmt DELETE_STMT = new SQLStmt("delete from contest_master where round_id = ? order by contest_id, round_id desc limit ?");
	
	private final VoltTable.ColumnInfo[] COLS = {
			new VoltTable.ColumnInfo("ROWS_LEFT", VoltType.INTEGER), 
			new VoltTable.ColumnInfo("ROWS_DELETED", VoltType.INTEGER)
	};
	private final VoltTable[] STATS = {new VoltTable(COLS)};
	
	public VoltTable[] run(int contestId, int round_id, int batchSize) {
		
		STATS[0].clearRowData();
		
		// Array to hold ROWS_LEFT and ROWS_DELETED
		Integer[] statValues = {0,0};
		
		voltQueueSQL(SELECT_STMT, round_id);
		VoltTable result = voltExecuteSQL()[0];
		
		if(result.advanceRow()) {
			statValues[0] = (int) result.getLong(0);
			
			if(statValues[0] != 0) {
				voltQueueSQL(DELETE_STMT, round_id, batchSize);
				result = voltExecuteSQL()[0];
				if(result.advanceRow()) {
					statValues[1] = (int) result.getLong(0);
				}
			}
		}
		
		STATS[0].addRow(statValues);
		return STATS;
	}
}
