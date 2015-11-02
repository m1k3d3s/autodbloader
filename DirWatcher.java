package com.mpd.app;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import static java.nio.file.StandardWatchEventKinds.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.PreparedStatement;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class DirWatcher {

    private Path path = null;
    private WatchService watchService = null;
    private String csvfile = null;
    public void init(String p) {
        path = Paths.get(p);
        try {
            watchService = FileSystems.getDefault().newWatchService();
            //path.register(watchService, ENTRY_CREATE,ENTRY_DELETE,
            	//	ENTRY_MODIFY, OVERFLOW);
            path.register(watchService, ENTRY_CREATE, ENTRY_MODIFY);
        } catch (IOException e) {
            System.out.println("IOException" + e.getMessage());
        }
    }

    public void doRounds() throws SQLException {
        WatchKey key = null;
        while(true) {
            try {
                    key = watchService.take();
                    for (WatchEvent<?> event : key.pollEvents()){
                        Kind<?> kind = event.kind();
                        System.out.println("Event on " + event.context().toString() + " is " + kind);
                        csvfile = event.context().toString();
                        //System.out.println(csvfile);
                        doDataLoad();
                    }
            } catch (InterruptedException e) {
                System.out.println("InterruptedException: " + e.getMessage());
            }
            boolean reset = key.reset();
            if(!reset)
                break;
        }
    }
    
    public void doDataLoad() throws SQLException {
    	String url = "jdbc:mysql://localhost:3306/feedback";
		String user = "sqluser";
		String pw = "sqlusermd";
		Connection connect = null;
		String sql = "LOAD DATA LOCAL INFILE '/historicaldata/csvfiles/"+csvfile+ "' INTO TABLE historicalprices FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES";
		Logger LOGGER = Logger.getLogger(DirWatcher.class.getName());
        LOGGER.setLevel(Level.INFO);
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connect = DriverManager.getConnection(url,user,pw);
			PreparedStatement pst = (PreparedStatement) connect.prepareStatement(sql);
			pst.execute();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e1) {
			// TODO Auto-generated catch block
            LOGGER.severe("Could not update database");
			e1.printStackTrace();
		}
    }
}
