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

import org.mappinganalysis.utils.Utils;

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
              `mappingName` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
			  PRIMARY KEY (`mapping_id`),
			    KEY `srcOntID` (`srcOntID`),
  				KEY `trgOntID` (`trgOntID`)
			) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
            
			CREATE TABLE `links` (
              `srcURL` varchar(150) COLLATE latin1_general_cs NOT NULL,
              `trgURL` varchar(150) COLLATE latin1_general_cs NOT NULL,
              `mapping_id_fk` int(11) DEFAULT NULL,
                KEY `srcURL` (`srcURL`),
  				KEY `trgURL` (`trgURL`)   
            ) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
            
		*/
		
		Connection con = Utils.openDbConnection();
		
		//create map with onto abbreviation and ontid
		String sql = "SELECT abbreviation, ontid FROM `ontologies`";
		PreparedStatement psmt = con.prepareStatement(sql);
		ResultSet rs = psmt.executeQuery();
		HashMap<String,Integer> ontos = new HashMap<>(); 
		while(rs.next()){
			ontos.put(rs.getString(1),rs.getInt(2));
		}
		rs.close();
		
		String dir = "data/mappings";
		File mappingDir	= new File (dir);
        
		File[] importFiles	= mappingDir.listFiles();
		for (File file : importFiles) {			
			if(!file.getName().equals(".svn")){
				System.out.println("######################################");
				//System.out.println("Import data from file '"+file+"'\n");
				
				String srcOnt		= file.getName().split("_")[0].split("\\[")[0];
				String srcOntAbbr	= file.getName().split("_")[0].split("\\[")[1].replace("]", "");
				String trgOnt		= file.getName().split("_")[1].split("\\[")[0];
				String trgOntAbbr	= file.getName().split("_")[1].split("\\[")[1].replace("].txt", "");

				//ignore LOINC
				if(!(srcOntAbbr.equals("LOINC") || trgOntAbbr.equals("LOINC"))){
					System.out.println("Import "+srcOnt + "["+srcOntAbbr+"] - "+trgOnt + "["+trgOntAbbr+"]");

					int srcOntID, trgOntID;
					
					//insert ontologies if not contained so far 
					if(!ontos.containsKey(srcOntAbbr)){
						srcOntID = insertOnto(con, srcOnt, srcOntAbbr);
						ontos.put(srcOntAbbr, srcOntID);
						System.out.println(srcOntAbbr +", ontoID = "+ srcOntID);
					}
					if(!ontos.containsKey(trgOntAbbr)){
						trgOntID = insertOnto(con, trgOnt, trgOntAbbr);
						ontos.put(trgOntAbbr, trgOntID);
						System.out.println(trgOntAbbr +", ontoID = "+ trgOntID);
					}
					
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
						    		"SELECT DISTINCT srcURL, trgURL, ? " + //eliminate duplicates (case sensitive decision!)
						    		"FROM `tmp_links` tl " +
						    		"WHERE method = ? " ;
						    		//check that link has not been inserted previously
						    		/*"AND NOT EXISTS (SELECT srcURL, trgURL, mapping_id_fk " +
										    		"FROM links l2 " +
										    		"WHERE l2.`srcURL` = tl.`srcURL` " +
										    		"AND l2.`trgURL` = tl.`trgURL` " +
										    		"AND l2.`mapping_id_fk` = ? )";*/
				    
				    PreparedStatement psmt3 = con.prepareStatement(query5);
					System.out.println("Import links from tmplinks ..");
					System.out.println("Methods:");
			
					int rowcount = 0;
				    for(String m : methodMapping.keySet()){
				    	System.out.println("\t- "+m);
					    psmt3.setInt(1,methodMapping.get(m));
					    psmt3.setString(2,m);
					    //psmt3.setInt(3,methodMapping.get(m));
					    try {
					    	rowcount = psmt3.executeUpdate(); 
					    	System.out.println(rowcount + " rows inserted!");
						} catch (SQLException e) {
							System.out.println(psmt3.toString());
							System.err.println(e);
						}
				    }
				    psmt3.close();
				    fr.close();
				    br.close();
				}
			}		
		}
	    dropTmpTable(con);
	    con.close();
	    
	    System.out.println("Notes:\n" +
						"- already contained links (i.e. same srcURL, same trgURL, same method) have been ignored!\n"+
						"- links created by different methods, are inserted in different mappings:");
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
				String printStr = "";
				
				if(!rs3.next()){ //if mapping not contained, insert new mapping metadata
					String query4 = "INSERT INTO `mappings` (srcOntID, trgOntID, method, mappingName) " +
									"VALUES (?,?,?,?)";
					
					PreparedStatement psmt2 = con.prepareStatement(query4);
					psmt2.setInt(1, ontos.get(srcOntAbbr));
					psmt2.setInt(2, ontos.get(trgOntAbbr));
					psmt2.setString(3, method);
					psmt2.setString(4, srcOntAbbr+"_"+trgOntAbbr+"_"+method);
					psmt2.executeUpdate();
					psmt2.close();
					
					//re-execute query3 to get mapping-id (has been created automatically)
					psmt.setString(1, srcOntAbbr);
					psmt.setString(2, trgOntAbbr);
					psmt.setString(3, method);
					rs3 = psmt.executeQuery();
					rs3.next();
					methodMapping.put(method,rs3.getInt(1));
					printStr+= "Mapping \""+srcOntAbbr+" - "+trgOntAbbr+" - "+method+"\" created!\n";
				}else{//mapping already inserted, save method id
					methodMapping.put(method,rs3.getInt(1));
					printStr+="Mapping \""+srcOntAbbr+" - "+trgOntAbbr+" - "+method+" already existed!\n";
				}
				System.out.println(printStr.substring(0,printStr.length()-2));
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
		//keep case sensitive URIs (srcURL, trgURL --> latin1_general_cs)
		String createTmpTable =	"CREATE TABLE `tmp_links` (" +
									"`srcURL` varchar(200) COLLATE latin1_general_cs NOT NULL, " +
									"`trgURL` varchar(200) COLLATE latin1_general_cs NOT NULL, " +
									"`method` varchar(50) COLLATE utf8_unicode_ci NOT NULL" +
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
