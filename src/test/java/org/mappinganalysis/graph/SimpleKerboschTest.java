package org.mappinganalysis.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class SimpleKerboschTest {

	public SimpleKerboschTest() {
		// TODO Auto-generated constructor stub
	}

	HashSet<Integer> nodes;
	HashMap<Integer, List<Integer>> edges;  
	
	
	/**
	 * test graph G = (V,E)
	 * V ={0,1,2,3,4,5,6} {0,1,2,3} is a clique as well as {3,4} and {4,5,6} 
	 * 
	 * 0--1    6
	 * |\ |   / |
	 * 2--3--4--5
	 */
	@Before
	public void setUp(){
		nodes = new HashSet<>();
		edges = new HashMap<>();
	
		nodes.add(0);nodes.add(1);nodes.add(2);nodes.add(3);nodes.add(4);nodes.add(5);nodes.add(6);
		List<Integer> n0 = new ArrayList<Integer>();
		List<Integer> n1 = new ArrayList<Integer>();
		List<Integer> n2 = new ArrayList<Integer>();
		List<Integer> n3 = new ArrayList<Integer>();
		List<Integer> n4 = new ArrayList<Integer>();
		List<Integer> n5 = new ArrayList<Integer>();
		List<Integer> n6 = new ArrayList<Integer>();
		
		n0.add(1);n0.add(2);n0.add(3);
		n1.add(0);n1.add(2);n1.add(3);
		n2.add(0);n2.add(1);n2.add(3);
		n3.add(0);n3.add(1);n3.add(2);n3.add(4);
		n4.add(3);n4.add(5);n4.add(6);
		n5.add(4);n5.add(6);
		n6.add(4);n6.add(5);
		edges.put(0, n0);
		edges.put(1, n1);
		edges.put(2, n2);
		edges.put(3, n3);
		edges.put(4, n4);
		edges.put(5, n5);
		edges.put(6, n6);
	}
	
	@Test
	public void testCliqueIdentification(){
		CliqueIdentification ci = new CliqueIdentification();
		Set<Set<Integer>> set = ci.simpleCluster(nodes, edges);
		Assert.assertEquals(3, set.size());
		for (Set<Integer> clique: set){
			if (clique.contains(0)){
				Assert.assertEquals(4, clique.size());
				Assert.assertTrue(clique.contains(1));
				Assert.assertTrue(clique.contains(2));
				Assert.assertTrue(clique.contains(3));
			}
			if (clique.contains(3)&& clique.size()==2){
				Assert.assertTrue(clique.contains(4));
			}
			if (clique.contains(6)){
				Assert.assertTrue(clique.contains(4));
				Assert.assertTrue(clique.contains(5));
			}
			
		}
	}
}
