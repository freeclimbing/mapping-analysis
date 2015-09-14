package org.mappinganalysis.graph;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConnectedComponentLoader {

	
	public ConnectedComponentSet loadAllCCsFromDB(Connection con) throws SQLException{
		return loadCCsFromDB(con, null);
	}
	
	public ConnectedComponentSet loadCCsFromDB(Connection con, Set<Integer> ccIDs) throws SQLException{
		
		ConnectedComponentSet ccSet = new ConnectedComponentSet();
		
		//get link set for current cc - both directions (--> union) 
		
		PreparedStatement psmt;
		ResultSet rs;
		String sql = ""; 
		
		if(ccIDs == null){
			System.out.println("Load all CCs from DB ..");
			
			sql = "SELECT DISTINCT l.srcID, l.trgID, cc.ccID " +
					"FROM connectedComponents cc, linksWithIDs l " +
					"WHERE (cc.`conceptID` = l.srcID " +
					"OR   cc.`conceptID` = l.trgID) " +
					"UNION " +
					"SELECT DISTINCT l.trgID, l.srcID, cc.ccID " +
					"FROM connectedComponents cc, linksWithIDs l " +
					"WHERE (cc.`conceptID` = l.srcID " +
					"OR   cc.`conceptID` = l.trgID );";
			psmt = con.prepareStatement(sql);
			rs = psmt.executeQuery();	
			
		}else{
			System.out.println("Load CCs "+ccIDs+" from DB ..");
			
			String psmtString = "?";
					
			for(int i = 0; i<ccIDs.size()-1;i++){
				psmtString+=",?";
			}
			
			sql = "SELECT DISTINCT l.srcID, l.trgID, cc.ccID " +
					"FROM connectedComponents cc, linksWithIDs l " +
					"WHERE (cc.`conceptID` = l.srcID " +
					"OR   cc.`conceptID` = l.trgID) " +
					"AND cc.ccID IN ("+psmtString+") " +
					"UNION " +
					"SELECT DISTINCT l.trgID, l.srcID, cc.ccID " +
					"FROM connectedComponents cc, linksWithIDs l " +
					"WHERE (cc.`conceptID` = l.srcID " +
					"OR   cc.`conceptID` = l.trgID ) " +
					"AND cc.ccID IN ("+psmtString+");";
			psmt = con.prepareStatement(sql);
			
			int index = 1;
			for(int ccID:ccIDs){
				psmt.setInt(index, ccID);
				psmt.setInt(index+ccIDs.size(), ccID);
				index++;
			}
			rs = psmt.executeQuery();		
			
		}
		while(rs.next()){

			int srcID = rs.getInt(1);
			int trgID = rs.getInt(2);
			int ccID  = rs.getInt(3);
			
			HashSet<Integer> nodes;
			HashMap<Integer, List<Integer>> edges; 
			
			Set<Integer> currentCCs = ccSet.getCCids();
			
			if(currentCCs.contains(ccID)){
				nodes = ccSet.getNodesForCC(ccID);
				edges = ccSet.getEdgesForCC(ccID);
				
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
			}else{//create new cc, inlc. new node&edge set
				
				nodes = new HashSet<Integer>();
				edges = new HashMap<Integer,List<Integer>>();
			
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
				
				ConnectedComponent cc = new ConnectedComponent(ccID, nodes, edges);
				ccSet.addCC(ccID, cc);
			}
		}			
		psmt.close();
		rs.close();
		System.out.println("Loaded "+ccSet.getSize()+" CCs from DB .. ");
		return ccSet;	
	}	
}
