package org.mappinganalysis.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;


public class CliqueIdentification {
Logger log = Logger.getLogger(getClass());

	private float threshold;
		
	public Set<Set<Integer>> simpleCluster(Set<Integer> nodes,
			HashMap<Integer,List<Integer>> edges){
		Set <Integer> r = new HashSet<Integer>();
		Set <Integer> x = new HashSet<Integer>();
		Set <Integer> p = new HashSet<Integer>();
		Set<Set<Integer>> clusters = new HashSet<Set<Integer>> ();
		for (int i : nodes){
			p.add(i);
		}
		this.simpleKerbosch(r, p, x, edges,clusters);
		return clusters;
	}
	
	private void simpleKerbosch(Set<Integer> r,Set<Integer> p, Set<Integer> x,HashMap<Integer,
			List<Integer>> edges, Set<Set<Integer>> clusters){
		if (p.isEmpty()&&x.isEmpty()){
			clusters.add(r);
			return ;
		}
		
		while (!p.isEmpty()){
			int n = p.iterator().next();
			Set<Integer> r2 = new HashSet<Integer>(r);
			r2.add(n);
			Set<Integer> p2 = new HashSet<Integer>(p);
			Set<Integer> x2 = new HashSet<Integer>(x);
			Set<Integer> neighbors = new HashSet<Integer>();
			for (int corNode: edges.get(n)){
				int neigh =(corNode);
				neighbors.add(neigh);
			}
			p2.retainAll(neighbors);
			x2.retainAll(neighbors);
			
			simpleKerbosch (r2,p2,x2,edges,clusters);
			p.remove(n);
			x.add(n);
		}
	}
	public float getThreshold() {
		return threshold;
	}
	public void setThreshold(float threshold) {
		this.threshold = threshold;
	}
}
