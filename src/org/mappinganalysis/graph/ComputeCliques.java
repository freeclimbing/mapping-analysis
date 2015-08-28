package org.mappinganalysis.graph;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComputeCliques {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	
	
	public static void main(String[] args) throws SQLException {
		
		String dbname = "bioportal_mappings_11_08_2015";
		String dbURL= "jdbc:mysql://dbserv2.informatik.uni-leipzig.de:3306/"+dbname;
		String user = "root";
		String pw = "wwwdblog!";
		Connection con = DriverManager.getConnection(dbURL, user, pw);
		
		//create map with onto abbreviation and ontid
		String sql = "SELECT DISTINCT conceptID, ccID " +
				"FROM connectedComponents " +
				"WHERE ccID = 18126 " +
				"ORDER BY ccID;";
		PreparedStatement psmt = con.prepareStatement(sql);
		ResultSet rs = psmt.executeQuery();
		HashMap<Integer,Integer> conceptToCCid = new HashMap<>();
		HashSet<Integer> ccSet = new HashSet<>();
		
		while(rs.next()){
			int conceptID = rs.getInt(1);
			int ccID = rs.getInt(2);
			conceptToCCid.put(conceptID, ccID);
			ccSet.add(ccID);
			//System.out.println(conceptID + "\t" + ccID);
		}
		rs.close();
		
		//get connected components
		for(int ccID:ccSet){

			HashSet<Integer> nodes = new HashSet<Integer>();
			HashMap<Integer, List<Integer>> edges = new HashMap<Integer,List<Integer>>();
			
			//get correspondence set for current cc - both directions (--> union) 
			sql = 	"(SELECT DISTINCT src.id, trg.id" +
					" FROM connectedComponents cc, `concept` c, links l, concept src, concept trg" +
					" WHERE cc.ccID = ?" +
					" AND cc.`conceptID` = c.`id`" +
					" AND (c.`url` = l.srcURL" +
					" OR   c.`url` = l.trgURL)" +
					" AND l.`srcURL` = src.`url`" +
					" AND l.`trgURL` = trg.`url`)" +
					" UNION (" +
					" SELECT DISTINCT trg.id, src.id" +
					" FROM connectedComponents cc, `concept` c, links l, concept src, concept trg" +
					" WHERE cc.ccID = ?" +
					" AND cc.`conceptID` = c.`id`" +
					" AND (c.`url` = l.srcURL" +
					" OR   c.`url` = l.trgURL)" +
					" AND l.`srcURL` = src.`url`" +
					" AND l.`trgURL` = trg.`url`)";
			
			
			psmt = con.prepareStatement(sql);
			psmt.setInt(1, ccID);
			psmt.setInt(2, ccID);
			rs = psmt.executeQuery();
			
			while(rs.next()){

				int srcID = rs.getInt(1);
				int trgID = rs.getInt(2);
				//System.out.println(srcID+"\t"+trgID);
				
				//fill node set
				nodes.add(srcID);
				
				//fill relationship set (node:list of node neighbors)
				if(!edges.containsKey(srcID)){
					List<Integer> neighbors = new ArrayList<Integer>();
					neighbors.add(trgID);
					edges.put(srcID, neighbors);
				}else{
					List<Integer> neighbors = edges.get(srcID);
					neighbors.add(trgID);
					edges.put(srcID, neighbors);
				}
			}
			rs.close();
			
			System.out.println("####");
			System.out.println("ccID = " + ccID);

			/*for(Integer nodeID : edges.keySet()){
				System.out.println(nodeID + ": " + edges.get(nodeID));
			}*/
			
			sql = "SELECT distinct id, url FROM concept;";
			psmt = con.prepareStatement(sql);
			rs = psmt.executeQuery();
			
			HashMap<Integer, String> idUrlMap = new HashMap<>();
			
			while (rs.next()) {
				idUrlMap.put(rs.getInt(1), rs.getString(2));
			}
			
			sql = "SELECT distinct url, attValue FROM concept_attributes " +
				  "WHERE attName = \"label\";";
			psmt = con.prepareStatement(sql);
			rs = psmt.executeQuery();
			
			HashMap<String, String> urlLabelMap = new HashMap<>();
			
			while (rs.next()) {
				urlLabelMap.put(rs.getString(1), rs.getString(2));
			}			
			
			if(ccID != 124){ //ACHTUNG FUER 124 - Problem mit URI Eindeutigkeit 
				// compute cliques for CCs
				CliqueIdentification ci = new CliqueIdentification();
				Set<Set<Integer>> cliqueSet = ci.simpleCluster(nodes, edges);
				System.out.println("Cliques:");
				
				int cnt = 1;
				for(Set<Integer> clique : cliqueSet){
					
					StringBuilder cliqueString = new StringBuilder("\nClique "+cnt+":\n");
					for(int c : clique){
						String url = idUrlMap.get(c);
						String label = urlLabelMap.get(url);
						cliqueString.append(url+"\t"+label+"\n");
						
					}
					System.out.println(cliqueString);
					cnt++;
				}
			}
		}
		psmt.close();
		con.close();
	}
}
