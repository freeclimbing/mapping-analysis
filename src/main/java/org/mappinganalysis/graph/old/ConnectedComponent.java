package org.mappinganalysis.graph.old;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class ConnectedComponent {
	
	int ccID;
	HashSet<Integer> nodes = new HashSet<Integer>();
	HashMap<Integer, List<Integer>> edges = new HashMap<Integer,List<Integer>>();
	
	
	public ConnectedComponent(int ccID, HashSet<Integer> nodes, HashMap<Integer, List<Integer>> edges) {
		this.ccID = ccID;
		this.nodes = nodes;
		this.edges = edges;
	}
	
	public int getCcID() {
		return ccID;
	}
	public HashSet<Integer> getNodes() {
		return nodes;
	}
	public HashMap<Integer, List<Integer>> getEdges() {
		return edges;
	}
}
