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

public class ImportOntologiesToDB {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		
		/* 0) Create repository if not exists 'dbname'
		 * 1) Download ontology files using DownloadBioportalOntologies.java
		 * 2) Create the following table:
		 *  
			CREATE TABLE `concept_attributes` (
				  `url` varchar(150) COLLATE latin1_general_cs NOT NULL,
				  `attName` varchar(50) DEFAULT NULL,
				  `attValue` varchar(1000) DEFAULT NULL,
				  `ontID` int(11) NOT NULL,
				  KEY `url` (`url`)
			);
			
			3) Run this class (ImportOntologiesToDB.java).
		*/
		
		String dbname = "bioportal_mappings_11_08_2015";
		String dbURL= "jdbc:mysql://dbserv2.informatik.uni-leipzig.de:3306/"+dbname;
		String user = "root";
		String pw = "wwwdblog!";
		Connection con = DriverManager.getConnection(dbURL, user, pw);
		
		String dir = "data/ontologies";
		File ontoDir = new File (dir);
        
	    
		File[] importFiles	= ontoDir.listFiles();
		for (File file : importFiles) {
			
			if(!(file.getName().equals(".svn")||file.getName().equals("originalFiles"))){
							
				//extract ontoName from file name
				String ontoName = file.getName().split("_")[0];

				System.out.println("##############"+ontoName+"##############");
				dropTmpTable(con);
				createTmpTable(con);

				//get onto id 
				int oid = getOntologyID(con,ontoName);
				
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
				
				System.out.println("Import "+ontoName +" " + attName);
				
				String sql = "LOAD DATA LOCAL INFILE " +
							"'"+file.getAbsolutePath().replace("\\", "/")+"' " +
							"INTO TABLE `tmp_attributes`;";
							
				con.prepareStatement(sql).executeUpdate();				
				System.out.println("Import to tmp table ..");
						
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
			    
				System.out.println("Imported "+cnt+" " +attName +"s to concept_attributes ..");
			    psmt.close();
			    fr.close();
			    br.close();
			}		
		}
	    dropTmpTable(con);
	    con.close();
	    System.out.println("\nDone!");
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
