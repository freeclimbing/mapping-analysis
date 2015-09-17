package org.mappinganalysis.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.mappinganalysis.utils.Utils;

public class GenerateConceptAndLinkTableWitIhDs {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		
		Connection con = Utils.openDbConnection();

		String sql = "DROP TABLE IF EXISTS `concept`;";
		PreparedStatement psmt = con.prepareStatement(sql);
		psmt.executeUpdate();
		
		sql =	"CREATE TABLE `concept` (" +
				"`id` int(50) NOT NULL AUTO_INCREMENT," +
				"`url` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL," +
				"`ontID_fk` int(11) NOT NULL," +
				"PRIMARY KEY (`id`)," +
				"KEY `url` (`url`));";
		
		psmt = con.prepareStatement(sql);
		int returnValue = psmt.executeUpdate();
		System.out.println("Table `concept` created .. "+returnValue +" rows returned.");
		
		//create ids for concepts covered by links
		sql = 	"INSERT INTO concept(url, ontID_fk) " +
				"SELECT DISTINCT url, oid FROM " +
				"(SELECT l.srcURL AS url, m.`srcOntID` AS oid FROM links l, mappings m " +
				"WHERE l.`mapping_id_fk` = m.`mapping_id` " +
				"UNION " +
				"SELECT l.trgURL AS url, m.`trgOntID` AS oid FROM links l, mappings m " +
				"WHERE l.`mapping_id_fk` = m.`mapping_id` ) a;";

		psmt = con.prepareStatement(sql);
		returnValue = psmt.executeUpdate();
		System.out.println("Inserted "+returnValue +" rows into `concept`.");
		
		//create ids for other concepts (rest from `concept_attributes`)
		sql = 	"INSERT INTO concept(url, ontID_fk) " +
				"SELECT DISTINCT url AS url, ontID FROM `concept_attributes`" +
				"WHERE url NOT IN (SELECT DISTINCT url u FROM concept);";
		
		psmt = con.prepareStatement(sql);
		returnValue = psmt.executeUpdate();
		System.out.println("Inserted "+returnValue +" rows into `concept`.");
		
		//create link table based on ids instead of URLs
		sql = "DROP TABLE IF EXISTS `linksWithIDs`;";
		psmt = con.prepareStatement(sql);
		psmt.executeUpdate();
		
		sql =	"CREATE TABLE `linksWithIDs` (" +
				"  `srcID` int(11) NOT NULL," +
				"  `trgID` int(11) NOT NULL," +
				"  `map_id_fk` int(11) DEFAULT NULL," +
				"  KEY `srcID` (`srcID`)," +
				"  KEY `trgID` (`trgID`));";
		
		psmt = con.prepareStatement(sql);
		returnValue = psmt.executeUpdate();
		System.out.println("Table `linksWithIDs` created .. "+returnValue +" rows returned.");
		
		sql = "INSERT INTO linksWithIDs " +
				"SELECT DISTINCT src.id, trg.id, l.`mapping_id_fk` " +
				"FROM links l,  mappings m, concept src, concept trg " +
				"WHERE l.`srcURL` = src.`url` " +
				"AND l.`mapping_id_fk` = m.`mapping_id` "+
				"AND m.`srcOntID` = src.ontID_fk "+
				"AND l.`trgURL` = trg.`url`" +
				"AND m.`trgOntID` = trg.ontID_fk ;";
		System.out.println(sql);
		
		psmt = con.prepareStatement(sql);
		returnValue = psmt.executeUpdate();
		System.out.println("Inserted "+returnValue +" rows into `linksWithIDs`.");
	}

}
