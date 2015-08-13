package org.mappinganalysis.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class ImportOntologiesToDB {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		
		/* Create the following table: 
			CREATE TABLE `concept_attributes` (
				  `url` varchar(150) COLLATE latin1_general_cs NOT NULL,
				  `attName` varchar(50) DEFAULT NULL,
				  `attValue` varchar(1000) DEFAULT NULL,
				  `ontID` int(11) NOT NULL,
				  KEY `url` (`url`)
			);
		*/
		
		String dbURL= "jdbc:mysql://dbserv2.informatik.uni-leipzig.de:3306/bioportal_mappings_11_08_2015";
		String user = "root";
		String pw = "wwwdblog!";
		Connection con = DriverManager.getConnection(dbURL, user, pw);
		
		String dir = "data/ontologies";
		File ontoDir = new File (dir);
        
	    
		File[] importFiles	= ontoDir.listFiles();
		for (File file : importFiles) {
			dropTmpTable(con);
			createTmpTable(con);
			
			if(!(file.getName().equals(".svn")||file.getName().equals("original_files"))){
				System.out.println("######################################");
				
				//extract ontoName from file name
				String ontoName = file.getName().split("_")[0];
				int oid = getOntologyID(con,ontoName);
				System.out.println(ontoName+" (oid = "+oid+")");
				
				//extract attribute type from file name
				String type = file.getName().split("_")[1].replace(".txt", "");
				String attName;
				if(type.equals("uriLabel")){
					attName = "label";
				}else if(type.equals("uriObsStatus")){
					attName = "obsolete";
				}else if(type.equals("uriSyn")){
					attName = "synonym";
				}else {
					attName = "other";
				}
				
				String sql = "LOAD DATA LOCAL INFILE " +
							"'"+file.getAbsolutePath().replace("\\", "/")+"' " +
							"INTO TABLE `tmp_attributes`;";
							
				int lineNumber = con.prepareStatement(sql).executeUpdate();				
				System.out.println("Imported "+lineNumber+" links to tmp table ..");
						
				FileReader fr = new FileReader(file);
			    BufferedReader br = new BufferedReader(fr);
		  
			    System.out.println("Insert "+attName+" ..");
			    sql = "INSERT INTO `concept_attributes` (url, attName, attValue, ontID) " +
					    		"SELECT DISTINCT url, ?, attValue, ? " + 
					    		"FROM `tmp_attributes` tl ";
					    		
			    PreparedStatement psmt = con.prepareStatement(sql);
			    psmt.setString(1, attName);
			    psmt.setInt(2, oid);
			    int cnt = psmt.executeUpdate();
			    
				System.out.println("Imported "+cnt+" attributes to concept_attributes ..");
			    psmt.close();
			    fr.close();
			    br.close();
			}		
		}
	    dropTmpTable(con);
	    con.close();
	    System.out.println(".. done!");
	}

	private static int getOntologyID(Connection con, String ontoName) throws SQLException {
		String sql = "SELECT ontid FROM `ontologies` WHERE abbreviation = ?";
		PreparedStatement psmt = con.prepareStatement(sql);
		psmt.setString(1, ontoName);
		ResultSet rs = psmt.executeQuery();
		rs.next();
		int oid = rs.getInt(1);
		psmt.close();
		return oid;
	}
	private static void dropTmpTable(Connection con) {
	    String dropTmpTable = "DROP TABLE IF EXISTS `tmp_attributes`;";
	    try {
			con.createStatement().executeUpdate(dropTmpTable);
			System.out.println("Dropped tmp table ..");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	private static void createTmpTable(Connection con) {
		//keep case sensitive URIs (srcURL, trgURL --> latin1_general_cs)
		String createTmpTable =	"CREATE TABLE `tmp_attributes` (" +
									"`url` varchar(200) COLLATE latin1_general_cs NOT NULL, " +
									"`attValue` varchar(1000) COLLATE latin1_general_cs NOT NULL " +
								") ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;";		
		try {
			con.createStatement().executeUpdate(createTmpTable);
			System.out.println("Created tmp table ..");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
