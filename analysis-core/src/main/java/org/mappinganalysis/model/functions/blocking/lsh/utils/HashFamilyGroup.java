package org.mappinganalysis.model.functions.blocking.lsh.utils;

import java.io.Serializable;
import java.util.*;


/**
 * Class that represents a group of hash families of the same type, i.e. {@link BitSetHashFunction}s.
 * 
 * @author mfranke
 *
 * @param <T>
 * 		-> the type of hash function in this family that implements {@link BitSetHashFunction}.
 * 
 * @param <U>
 * 		-> the return type of the hash function in this family.
 */
public class HashFamilyGroup<T extends BitSetHashFunction<U>, U> implements Serializable {

	private static final long serialVersionUID = -723972392381052855L;
	private List<HashFamily<T, U>> hashFamilies;
	
	/**
	 * Creates a new {@link HashFamilyGroup} object.
	 */
	public HashFamilyGroup(int numberOfHashFamilies){
		this.hashFamilies = new ArrayList<HashFamily<T, U>>(numberOfHashFamilies);
	}
	
	/**
	 * Creates a new {@link HashFamilyGroup} object.
	 * 
	 * @param hashFamilies
	 * 		-> a {@link List} of {@link HashFamily} objects of the same type.
	 */
	public HashFamilyGroup(List<HashFamily<T, U>> hashFamilies){
		this.hashFamilies = hashFamilies;
	}
	
	public int getNumberOfHashFamilies(){
		return this.hashFamilies.size();
	}
	
	public void addHashFamily(HashFamily<T, U> hashFamily){
		this.hashFamilies.add(hashFamily);
	}

	public List<HashFamily<T, U>> getHashFamilies() {
		return this.hashFamilies;
	}

	public void setHashFamilies(List<HashFamily<T, U>> hashFamilies) {
		this.hashFamilies = hashFamilies;
	}
	
	public HashFamily<T, U> getHashFamilyAt(int position){
		return this.hashFamilies.get(position);
	}
	
	/**
	 * Calculates the hashes for all hash functions in all families of this hash family group.
	 * 
	 * @param bitset
	 * 		-> the {@link BitSet} object to calculate to hashes for.
	 * 
	 * @return
	 * 		-> {@link List} of a {@link List} of hash values.
	 */	
	public List<List<U>> calculateHashes(BitSet bitset){
		final int numberOfHashFamilies = this.hashFamilies.size();
		
		final List<List<U>> hashes = new ArrayList<List<U>>(numberOfHashFamilies);
		
		for (int i = 0; i < numberOfHashFamilies; i++){
			final HashFamily<T, U> hashFamily = this.hashFamilies.get(i);
			final List<U> hashFamilyHashValues = hashFamily.calculateHashes(bitset);
			hashes.add(hashFamilyHashValues);
		}
		
		return hashes;
	}
	
	
	public static Integer[][] selectRandomPositions(int numberOfFamilies, int numberOfHashesPerFamily, int valueRange){

		final Set<Set<Integer>> res = new LinkedHashSet<Set<Integer>>(numberOfFamilies);
		int seed = 42;
		
		for (int i = 0; i < numberOfFamilies; i++){
			Set<Integer> rndPositions = HashFamily.selectRandomPositions(numberOfHashesPerFamily, valueRange, seed);
					
			while(res.contains(rndPositions)){				
				rndPositions = HashFamily.selectRandomPositions(numberOfHashesPerFamily, valueRange, seed++);
			}
			
			res.add(rndPositions);
		}
				
		final Integer[][] result = new Integer[numberOfFamilies][numberOfHashesPerFamily];
		int i = 0;
		for (Set<Integer> positionSet : res){
			result[i] = positionSet.toArray(new Integer[positionSet.size()]);
			i++;
		}

		return result;
	}
	
	
	/**
	 * Generates a group of {@link IndexHash} functions in a {@link HashFamily} with random indices.
	 * 
	 * @param numberOfFamilies
	 * 		-> count of hash families in this group.
	 * 
	 * @param numberOfHashesPerFamily
	 * 		-> count of hash functions in each family.
	 * 
	 * @param valueRange
	 * 		-> the output range for the {@link IndexHash} functions.
	 * 
	 * @return
	 * 		-> the hash family group of random {@link IndexHash} functions.
	 */
	public static HashFamilyGroup<IndexHash, Boolean> generateRandomIndexHashFamilyGroup(
			int numberOfFamilies, int numberOfHashesPerFamily, int valueRange){
				
		return generateRandomIndexHashFamilyGroup(numberOfFamilies, numberOfHashesPerFamily, valueRange, null);
	}
	
	public static HashFamilyGroup<IndexHash, Boolean> generateRandomIndexHashFamilyGroup(
			int numberOfFamilies, int numberOfHashesPerFamily, int valueRange, List<Integer> bits){
			
		final HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup = 
				new HashFamilyGroup<IndexHash, Boolean>(numberOfFamilies);
		
		for (int i = 0; i < numberOfFamilies; i++){
			hashFamilyGroup.addHashFamily(
					HashFamily.generateRandomIndexHashFamily(numberOfHashesPerFamily, valueRange, bits)
			);
		}
		
		return hashFamilyGroup;
	}
	
	public static HashFamilyGroup<IndexHash, Boolean> fromPositions(Integer[][] keyPositions){
		final int numberOfFamilies = keyPositions.length;
		
		final HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup = 
				new HashFamilyGroup<IndexHash, Boolean>(numberOfFamilies);
		
		for (int i = 0; i < numberOfFamilies; i++){
			hashFamilyGroup.addHashFamily(
				HashFamily.fromPositions(keyPositions[i])
			);
		}
		
		return hashFamilyGroup;
		
		
		
	}
}