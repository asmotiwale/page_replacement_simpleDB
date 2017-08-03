package simpledb.buffer;
import simpledb.file.FileMgr;
import simpledb.file.Statistics;
import simpledb.server.*;


public class BufferMgrStatistics extends BufferMgr{
	public BufferMgrStatistics(int numbuffers) {
		super(numbuffers);
		// TODO Auto-generated constructor stub
	}

	static BufferMgr bm = SimpleDB.bufferMgr();

	public static void getBufferStatistics(){
		 bm.getBufferStats();
	}
}
