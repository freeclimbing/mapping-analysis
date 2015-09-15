package org.mappinganalysis.graph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.mappinganalysis.utils.Utils;

public class ComputeCliques {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	
	
	public static void main(String[] args) throws SQLException {
		

		Connection con = Utils.openDbConnection();
		
		//load connected components from db
		ConnectedComponentLoader l = new ConnectedComponentLoader();
		
	
		Set<Integer> ccIDs = null;
		ConnectedComponentSet ccSet = null;
		
		boolean printWithLabels = false; //load metadata for all concepts takes about 20-30 sec (for bioportal dataset)
		boolean runAll = false;
		//true: run clique computation either for all available CCs in DB 
		if(runAll){
			ccSet = l.loadAllCCsFromDB(con);
			ccIDs = ccSet.getCCids();
		}
		//false: or for some CCs (adapt integer array!)
		else{
			ccIDs = new HashSet<>();
			List<Integer> ccList = Arrays.asList(new Integer[] { 18126, 1, 2, 3, 4});
			ccIDs = new HashSet<Integer>(ccList);
			ccSet = l.loadCCsFromDB(con,ccIDs);
			ccIDs = ccSet.getCCids(); //only use existing CCids contained in the result (in case you looked for a cc that does not exit)
			System.out.println(ccIDs);
		}		
		
		long start = System.currentTimeMillis();
		HashMap<Integer, List<String>> idUrlLabelMap = null;
		if(printWithLabels){
			System.out.println("\nGet some metadata ..");
			
			HashSet<Integer> allNodes = new HashSet<>();
			for(int ccID:ccIDs){
				allNodes.addAll(ccSet.getCC(ccID).getNodes());
			}
			
			if(allNodes!=null){
				idUrlLabelMap = getIdUrlLabelMap(con, allNodes);
			}
		}
		System.out.println( "Time to load metadata: "+ ((System.currentTimeMillis() - start)/1000) + " sec\n");
		
		// compute cliques for CCs
		System.out.println("Compute cliques .. ");
		int stopLoop = 0; 
		for(int ccID:ccIDs){
		
			System.out.println("####");
			System.out.println("ccID = " + ccID);
	
				
			System.out.println("Nodes: "+ccSet.getNodesForCC(ccID));
			System.out.println("Edges: "+ccSet.getEdgesForCC(ccID));
			
			CliqueIdentification ci = new CliqueIdentification();
			Set<Set<Integer>> cliqueSet = ci.simpleCluster(ccSet.getNodesForCC(ccID), ccSet.getEdgesForCC(ccID));
			System.out.println("Cliques:");
			
			int cnt = 1;
			for(Set<Integer> clique : cliqueSet){
				
				StringBuilder cliqueString = new StringBuilder("\nClique "+cnt+":\n");
				for(int c : clique){
					
					if(idUrlLabelMap!=null){
						cliqueString.append(c);
						try{
							String url = idUrlLabelMap.get(c).get(0);
							cliqueString.append("\t"+url);
						}catch (Exception e){
							cliqueString.append("\tno url available");
						}
						try{
							String label = idUrlLabelMap.get(c).get(1);
							cliqueString.append("\t"+label+"\n");
						}catch (Exception e){
							cliqueString.append("\tno label available\n");
						}
					}else{
						cliqueString.append(c+"\n");
					}
				}
				System.out.println(cliqueString);
				cnt++;
			}
			stopLoop++;
			if(stopLoop==100){
				break;
			}
		}
		con.close();
	}
	
	
	private static HashMap<Integer, List<String>> getIdUrlLabelMap(Connection con, HashSet<Integer> allNodeIds)
			throws SQLException {
		
		String psmtString = "?";
		for(int i = 0; i<allNodeIds.size()-1;i++){
			psmtString+=",?";
		}
		String sql1 = "SELECT distinct id, url " +
						"FROM concept " +
						"WHERE id IN ("+psmtString+");";
		
		String sql2 = "SELECT distinct cc.id, cc.url, a.attValue " +
					  "FROM concept cc JOIN concept_attributes a ON (cc.url = a.url) " +
					  "WHERE a.attName = \"label\" AND cc.id IN ("+psmtString+");";
				
		PreparedStatement psmt1 = con.prepareStatement(sql1);
		PreparedStatement psmt2 = con.prepareStatement(sql2);
		
		int index = 1;
		for(int n:allNodeIds){
			psmt1.setInt(index, n);			
			psmt2.setInt(index, n);	
			index++;
		}
		//System.out.println(psmt);
		ResultSet rs1 = psmt1.executeQuery();
		ResultSet rs2 = psmt2.executeQuery();
		
		HashMap<Integer, List<String>> idUrlLabelMap = new HashMap<>();
		
		while (rs1.next()) {
			List<String> l = new Vector<>();
			l.add(rs1.getString(2)); // [url]
			idUrlLabelMap.put(rs1.getInt(1), l); // (id,[url])
		}
		while (rs2.next()) {
			List<String> l = idUrlLabelMap.get(rs2.getInt(1));
			l.add(rs2.getString(3));//(id,[url,label])
		}
		psmt1.close();
		psmt2.close();
		rs1.close();
		rs2.close();
		
		return idUrlLabelMap;
	}	
}
