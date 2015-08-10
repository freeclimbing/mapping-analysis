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

public class ImportMappingsToDB {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws SQLException, IOException {
		
		/* For first run: Create new database and the following tables: 
		 * 
		 * 	
		  	CREATE TABLE `ontologies` (
		  	      `ontID` int(11) NOT NULL AUTO_INCREMENT,
				  `abbreviation` varchar(100) NOT NULL,
				  `fullname` varchar(100) DEFAULT NULL,
				  PRIMARY KEY (`ontID`),
				  UNIQUE KEY `abbreviation` (`abbreviation`),
				  UNIQUE KEY `fullname` (`fullname`)
			) ENGINE=MyISAM DEFAULT CHARSET=latin1;
			
			CREATE TABLE `mappings` (
			  `mapping_id` int(11) NOT NULL AUTO_INCREMENT,
			  `srcOntID` int(11) DEFAULT NULL,
			  `trgOntID`int(11) NULL,
			  `method` varchar(50) COLLATE utf8_unicode_ci NOT NULL,
			  PRIMARY KEY (`mapping_id`)
			) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
			
			CREATE TABLE `links` (
			  `srcURL` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
			  `trgURL` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
			  `mapping_id_fk` int(11) DEFAULT NULL,
			  UNIQUE(`srcURL`,`trgURL`,`mapping_id_fk`)
			) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
		*/
		
		
		//String dbURL= "jdbc:mysql://dbserv2.informatik.uni-leipzig.de:3306/bioportal_mappings_07-2015";
		String dbURL= "jdbc:mysql://dbserv2.informatik.uni-leipzig.de:3306/bioportal_mappings_08_2015";
		String user = "root";
		String pw = "wwwdblog!";
		Connection con = DriverManager.getConnection(dbURL, user, pw);
		
		//create map with onto abbreviation and ontid
		String sql = "SELECT abbreviation, ontid FROM `ontologies`";
		PreparedStatement psmt = con.prepareStatement(sql);
		ResultSet rs = psmt.executeQuery();
		HashMap<String,Integer> ontos = new HashMap<>(); 
		while(rs.next()){
			ontos.put(rs.getString(1),rs.getInt(2));
		}
		rs.close();
		
		String dir = "mappings";
		File mappingDir	= new File (dir);
        
		File[] importFiles	= mappingDir.listFiles();
		for (File file : importFiles) {			
			
			String srcOnt		= file.getName().split("_")[0].split("\\[")[0];
			String srcOntAbbr	= file.getName().split("_")[0].split("\\[")[1].replace("]", "");
			String trgOnt		= file.getName().split("_")[1].split("\\[")[0];
			String trgOntAbbr	= file.getName().split("_")[1].split("\\[")[1].replace("].txt", "");
			int srcOntID, trgOntID;
			
			//insert ontologies if not contained so far 
			if(!ontos.containsKey(srcOntAbbr)){
				srcOntID = insertOnto(con, srcOnt, srcOntAbbr);
				ontos.put(srcOntAbbr, srcOntID);
				System.out.println(srcOntAbbr +" -->"+ srcOntID);
			}
			if(!ontos.containsKey(trgOntAbbr)){
				trgOntID = insertOnto(con, trgOnt, trgOntAbbr);
				ontos.put(trgOntAbbr, trgOntID);
				System.out.println(trgOntAbbr +" -->"+ trgOntID);
			}
			System.out.println(srcOnt);
			System.out.println(srcOntAbbr);
			System.out.println(trgOnt);
			System.out.println(trgOntAbbr);
		
			System.out.println("Import data from file '"+file+"'\n");	
			
			//save form metadata in db
			dropTmpTable(con);
			createTmpTable(con);
			
			sql =	"LOAD DATA LOCAL INFILE " +
							"'"+file.getAbsolutePath().replace("\\", "/")+"' " +
							"INTO TABLE `tmp_links` " +
							"IGNORE 2 LINES;";
					
			int lineNumber = con.prepareStatement(sql).executeUpdate();
			System.out.println("Imported "+lineNumber+" links to tmp table ..");
					
			//get distinct methods in file
			HashMap<String, Integer> methodMapping = createMappingMetadata(con, srcOntAbbr, trgOntAbbr, ontos);
			
			FileReader fr = new FileReader(file);
		    BufferedReader br = new BufferedReader(fr);
	  
		    String query5 = "INSERT INTO `links` (srcURL, trgURL, mapping_id_fk) " +
				    		"SELECT srcURL, trgURL, ? " +
				    		"FROM `tmp_links` tl " +
				    		"WHERE method = ? " +
				    		"AND NOT EXISTS (SELECT srcURL, trgURL, mapping_id_fk " +
								    		"FROM links l2 " +
								    		"WHERE l2.`srcURL` = tl.`srcURL` " +
								    		"AND l2.`trgURL` = tl.`trgURL` " +
								    		"AND l2.`mapping_id_fk` = ? )";
		    
		    PreparedStatement psmt3 = con.prepareStatement(query5);
			System.out.println("Import links from tmplinks .. \nNotes:\n" +
					"- already contained links (i.e. same srcURL, same trgURL, same method) are ignored!\n"+
					"- links created by different methods, are inserted in different mappings:");
	
		    for(String m : methodMapping.keySet()){
		    	System.out.println("\t- "+m);
			    psmt3.setInt(1,methodMapping.get(m));
			    psmt3.setString(2,m);
			    psmt3.setInt(3,methodMapping.get(m));
			    try {
			    	psmt3.executeUpdate(); 
				} catch (SQLException e) {
					System.out.println(psmt3.toString());
					System.err.println(e);
				}
		    }
		    psmt3.close();
		    fr.close();
		    br.close();
		}
	    con.close();
	    System.out.println(".. done!");
	    
	    
	}

	private static HashMap<String, Integer> createMappingMetadata(Connection con, String srcOntAbbr, String trgOntAbbr, HashMap<String, Integer> ontos) {
		String query2 = "SELECT DISTINCT method FROM tmp_links"; 
		ResultSet rs2;
		HashMap<String, Integer> methodMapping = new HashMap<String, Integer>();
		
		try {
			rs2 = con.prepareStatement(query2).executeQuery();
			//create mapping metadata
			while(rs2.next()){
				
				String method = rs2.getString(1);
				
				String sql = "SELECT mapping_id FROM `mappings` m, `ontologies` s, `ontologies` t " +
							  "WHERE m.`srcOntID` = s.`ontID` " +
							  "AND m.`trgOntID`= t.`ontID` " +
							  "AND s.`abbreviation` = ? " +
							  "AND t.`abbreviation` = ? " +
							  "AND method = ?";
				
				PreparedStatement psmt = con.prepareStatement(sql);
				psmt.setString(1, srcOntAbbr);
				psmt.setString(2, trgOntAbbr);
				psmt.setString(3, method);
				
				ResultSet rs3 = psmt.executeQuery();
				String printStr = "\nCheck if mapping \""+srcOntAbbr+" - "+trgOntAbbr+" - "+method+"\" already exists ..";
				
				if(!rs3.next()){ //if mapping not contained, insert new mapping metadata
					String query4 = "INSERT INTO `mappings` (srcOntID, trgOntID, method) " +
									"VALUES (?,?,?)";
					
					PreparedStatement psmt2 = con.prepareStatement(query4);
					psmt2.setInt(1, ontos.get(srcOntAbbr));
					psmt2.setInt(2, ontos.get(trgOntAbbr));
					psmt2.setString(3, method);//
					psmt2.executeUpdate();
					psmt2.close();
					
					//re-execute query3 to get mapping-id (has been created automatically)
					psmt.setString(1, srcOntAbbr);
					psmt.setString(2, trgOntAbbr);
					psmt.setString(3, method);
					rs3 = psmt.executeQuery();
					rs3.next();
					methodMapping.put(method,rs3.getInt(1));
					printStr+=" no --> mapping created!\n";
				}else{//mapping already inserted, save method id
					methodMapping.put(method,rs3.getInt(1));
					printStr+=" yes!\n";
				}
				System.out.println(printStr);
				psmt.close();
				rs3.close();
			}
			rs2.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return methodMapping;
	}

	private static int insertOnto(Connection con, String ont, String ontAbbr) {
		String sql = "INSERT INTO `ontologies` (abbreviation, fullname) VALUES (?,?)";
		PreparedStatement psmt;
		int oid = -1;
		try {
			psmt = con.prepareStatement(sql);
			psmt.setString(1, ontAbbr);
			psmt.setString(2, ont);
			psmt.executeUpdate();
			
			sql = "SELECT ontid FROM `ontologies` WHERE abbreviation = ?";
			psmt = con.prepareStatement(sql);
			psmt.setString(1, ontAbbr);
			ResultSet rs = psmt.executeQuery();
			rs.next();
			oid = rs.getInt(1);
			psmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return oid;
	}

	private static void dropTmpTable(Connection con) {
	    String dropTmpTable = "DROP TABLE IF EXISTS `tmp_links`;";
	    try {
			con.createStatement().executeUpdate(dropTmpTable);
			System.out.println("Dropped tmp table ..");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	private static void createTmpTable(Connection con) {
		String createTmpTable =	"CREATE TABLE `tmp_links` (" +
									"`srcURL` varchar(200) COLLATE utf8_unicode_ci NOT NULL, " +
									"`trgURL` varchar(200) COLLATE utf8_unicode_ci NOT NULL, " +
									"`method` varchar(50) COLLATE utf8_unicode_ci NOT NULL" +
								") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;";
		
		try {
			con.createStatement().executeUpdate(createTmpTable);
			System.out.println("Created tmp table ..");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
