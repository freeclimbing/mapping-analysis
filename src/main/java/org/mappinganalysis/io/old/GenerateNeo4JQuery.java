package org.mappinganalysis.io.old;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.mappinganalysis.utils.Utils;

public class GenerateNeo4JQuery {

	/**
	 * Generate Neo4J CREATE query for a set of connected componentIDs.
	 */

    public static final String dbName = Utils.BIO_DB_NAME;

	public static void main(String[] args) throws SQLException {

		//delete all NODES + EDGES
		String neo4jQuery = "MATCH (n) \n"+
					  		"OPTIONAL MATCH (n)-[r]-() \n" +
					  		"DELETE n,r; \n\n";
		
		//for import of NODES + EDGDES for a set of connected componentds
		neo4jQuery += "CREATE \n";

		int[] componentIDs = {124,20141,18126,16745,16726};
		String nodeTupleString = sqlGetNodesForComponent(componentIDs);
		
		String edgeTupleString = sqlGetEdgesForComponent(componentIDs);
				
		neo4jQuery += nodeTupleString+",\n"+edgeTupleString+";\n\n";
		
		//get all NODES + EDGES = wie "SELECT * ..." wenn nur CC of interest in Neo4J-DB enthalten ist
		
		neo4jQuery += "MATCH (p:uri) RETURN p;";
				
		System.out.println(neo4jQuery);
	}

	private static String sqlGetEdgesForComponent(int[] componentIDs) throws SQLException {
		
		Connection con = Utils.openDbConnection(dbName);
		String psmtString = "?";
		for(int i = 0; i<componentIDs.length-1;i++){
			psmtString+=",?";
		}

		//shorten URLs = remove prefixes with mysql string function REPLACE
		//otherwise neo4j visualization is confusing
						
		String sql = "SELECT DISTINCT " +
						"CONCAT(\"(id\",CONVERT(l.srcID USING utf8),\")-[:EQUALS]->(id\", CONVERT(l.trgID USING utf8),\")\") AS neo4jEdge " +
						"FROM connectedComponents cc, linksWithIDs l " +
						"WHERE (cc.conceptID =l.srcID " +
						"OR   cc.conceptID =l.trgID ) " +
						"AND cc.ccID IN ("+psmtString+");";
		
		PreparedStatement psmt = con.prepareStatement(sql);
		
		try {
			for(int i = 0; i<componentIDs.length;i++){
				psmt.setInt(i+1, componentIDs[i]);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//System.out.println(psmt);
		ResultSet rs = psmt.executeQuery();
		
		String resultTupleString = "";
		
		while (rs.next()) {
			resultTupleString += rs.getString(1) + ",\n";
		}
		
		resultTupleString = resultTupleString.substring(0, resultTupleString.length()-2);
		
		psmt.close();
		rs.close();
		con.close();
		
		return resultTupleString;
		
	}

	private static String sqlGetNodesForComponent(int[] componentIDs) throws SQLException {
		
		Connection con = Utils.openDbConnection(dbName);
		String psmtString = "?";
		for(int i = 0; i<componentIDs.length-1;i++){
			psmtString+=",?";
		}

		//shorten URLs = remove prefixes with mysql string function REPLACE
		//otherwise neo4j visualization is confusing
		//some links might be contained in more than one mapping (different linking methods), e.g. SAME_URI, LOOM, CUI 
		//this effect is ignored when using DISTINCT!
		
		String sql = "SELECT DISTINCT " +
				"CONCAT (\"(id\",CONVERT(c.id USING utf8),\":uri { name : '\",o.abbreviation,\": \",a.attValue,\"'\",\"})\") AS neo4jNode " +
				"FROM connectedComponents cc, concept c, concept_attributes a, ontologies o " +
				"WHERE cc.conceptID = c.id " +
				"AND c.url = a.url " +
				"AND c.ontID_fk = a.ontID " +
				"AND c.ontID_fk = o.ontID " +
				"AND a.attName = \"label\" " +
				"AND cc.ccID  IN ("+psmtString+");";
		
		PreparedStatement psmt = con.prepareStatement(sql);
		
		try {
			for(int i = 0; i<componentIDs.length;i++){
				psmt.setInt(i+1, componentIDs[i]);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//System.out.println(psmt);
		ResultSet rs = psmt.executeQuery();
		
		String resultTupleString = "";
		
		while (rs.next()) {
			resultTupleString += rs.getString(1) + ",\n";
		}
		
		resultTupleString = resultTupleString.substring(0, resultTupleString.length()-2);
		
		psmt.close();
		rs.close();
		con.close();
		
		return resultTupleString;
	}
}
