package org.mappinganalysis.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.mappinganalysis.utils.Utils;

public class GenerateLinkTableWithIntIDs {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		
		Connection con = Utils.openDbConnection();
		
		String sql = "DROP TABLE IF EXISTS `linksWithIDs`;";
		PreparedStatement psmt = con.prepareStatement(sql);
		psmt.executeUpdate();
		
		sql =	"CREATE TABLE `linksWithIDs` (" +
				"  `srcID` int(11) NOT NULL," +
				"  `trgID` int(11) NOT NULL," +
				"  `map_id_fk` int(11) DEFAULT NULL," +
				"  KEY `srcID` (`srcID`)," +
				"  KEY `trgID` (`trgID`));";
		
		psmt = con.prepareStatement(sql);
		int returnValue = psmt.executeUpdate();
		System.out.println("Table `linksWithIDs` created .. "+returnValue +" rows returned.");
		
		sql = "INSERT INTO linksWithIDs " +
				"SELECT DISTINCT src.id, trg.id, l.`mapping_id_fk` " +
				"FROM links l, concept src, concept trg " +
				"WHERE l.`srcURL` = src.`url` " +
				"AND l.`trgURL` = trg.`url`;";
		psmt = con.prepareStatement(sql);
		returnValue = psmt.executeUpdate();
		System.out.println("Inserted "+returnValue +" rows into `linksWithIDs`.");
	}

}
