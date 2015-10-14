package org.mappinganalysis.io;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.mappinganalysis.utils.Utils;

public class ImportFlinkResults {

	/**
	 * Import results of connected component computation with flink
	 */

	public static final String dbName = Utils.BIO_DB_NAME; // BIO_DB_NAME, GEO_PERFECT_DB_NAME

	public static void main(String[] args) throws SQLException, IOException {
		
		Connection con = Utils.openDbConnection(dbName);
		//create table to import CCs (drop before if exists)
		
		String sql = "DROP TABLE IF EXISTS `connectedComponents`;";
		PreparedStatement psmt = con.prepareStatement(sql);
		psmt.executeUpdate();
		
		sql = "CREATE TABLE `connectedComponents` (" +
				"  `conceptID` int(11) DEFAULT NULL," +
				"  `ccID` int(11) DEFAULT NULL," +
				"  KEY `conceptID` (`conceptID`)," +
				"  KEY `ccID` (`ccID`));";
		
		psmt = con.prepareStatement(sql);
		
		int returnValue = psmt.executeUpdate();
		System.out.println("Table `connectedComponents` created .. "+returnValue +" rows returned.");
		
		//navigate to flink result directory 
		String current = new File( "." ).getCanonicalPath().replace("\\", "/").replace(" ", "%20");
        String dir = current+Utils.FLINK_RESULT_PATH;
        File flinkResultDir	= new File (dir);
		File[] importFiles	= flinkResultDir.listFiles();
		//import all files to table `connectedComponents`
		for (File file : importFiles) {			
			if(!file.getName().equals(".svn")){
				String path = dir+file.getName();
				System.out.println(path);
				sql = 	"LOAD DATA LOCAL INFILE " +
						"\""+path+"\" " +
						"INTO TABLE `connectedComponents` " +
						"FIELDS TERMINATED BY \" \";";

				psmt = con.prepareStatement(sql);
				returnValue = psmt.executeUpdate();
				System.out.println("Inserted "+returnValue +" rows into `concept`.");
			}
		}
		psmt.close();
		con.close();
	}

}