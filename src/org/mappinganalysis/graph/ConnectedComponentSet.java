package org.mappinganalysis.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConnectedComponentSet {
	

	HashMap<Integer, ConnectedComponent> ccSet;
	
	public ConnectedComponentSet() {
		this.ccSet = new HashMap<>();
	}
	
	public void addCC(int ccID, ConnectedComponent cc){
		ccSet.put(ccID, cc);
	}
	
	public Set<Integer> getCCids() {
		return ccSet.keySet();
	}
	
	public ConnectedComponent getCC(int ccID) {
		return ccSet.get(ccID);
	}
	
	public HashSet<Integer> getNodesForCC(int ccID){
		return ccSet.get(ccID).getNodes();
	}
	
	public HashMap<Integer, List<Integer>> getEdgesForCC(int ccID){
		return ccSet.get(ccID).getEdges();
	}

	public int getSize() {
		return ccSet.size();
	}
	
}
