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
		
		//load connected components from db
		ConnectedComponentLoader l = new ConnectedComponentLoader();
		
	
		Set<Integer> ccIDs = null;
		ConnectedComponentSet ccSet = null;
		
		boolean printWithLabels = true; //load metadata takes about 20-25 sec
		boolean runAll = true;
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
		HashMap<Integer, String[]> idUrlLabelMap = null;
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
						try{
							String url = idUrlLabelMap.get(c)[0];
							String label = idUrlLabelMap.get(c)[1];
							cliqueString.append(url+"\t"+label+"\n");
						}catch (Exception e){
							System.out.println("No url/label available .. ? ");
							System.out.println(e);
						}
					}else{
						cliqueString.append(c+"\n");
					}
				}
				System.out.println(cliqueString);
				cnt++;
			}
		}
		con.close();
	}
	
	
	private static HashMap<Integer, String[]> getIdUrlLabelMap(Connection con, HashSet<Integer> allNodeIds)
			throws SQLException {
		
		String psmtString = "?";
		for(int i = 0; i<allNodeIds.size()-1;i++){
			psmtString+=",?";
		}
		String sql = "SELECT distinct c.id, c.url, a.attValue FROM concept c, concept_attributes a " +
						"WHERE c.url = a.url " +
						"AND a.attName = \"label\" " +
						"AND c.id IN ("+psmtString+");";
		
		PreparedStatement psmt = con.prepareStatement(sql);
		int index = 1;
		for(int n:allNodeIds){
			psmt.setInt(index, n);			
			index++;
		}
		ResultSet rs = psmt.executeQuery();
		
		HashMap<Integer, String[]> idUrlLabelMap = new HashMap<>();
		
		while (rs.next()) {
			String[] a = {rs.getString(2), rs.getString(3)};
			idUrlLabelMap.put(rs.getInt(1), a);

		}
		psmt.close();
		rs.close();
		
		return idUrlLabelMap;
	}	
}
