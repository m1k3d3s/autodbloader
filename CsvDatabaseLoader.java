package com.mpd.app;

import java.sql.SQLException;

public class CsvDatabaseLoader {
	public static void main(String[] args) throws SQLException{
		String p = "/historicaldata/csvfiles";
		DirWatcher dirwatcher = new DirWatcher();
		dirwatcher.init(p);
		dirwatcher.doRounds();
		dirwatcher.doDataLoad();
	}
	
}
