package simpledb.file;

import simpledb.server.*;



public class FileMgrStatistics extends FileMgr {
	public FileMgrStatistics(String dbname) {
		super(dbname);
	}
	
	static FileMgr fm = SimpleDB.fileMgr();

	public static Statistics getFileStatistics(){
		return fm.stats;
	}
	
}
