package org.mappinganalysis.model.functions.blocking.lsh.structure;

import org.mappinganalysis.model.functions.blocking.lsh.utils.BitSetHashFunction;

import java.io.Serializable;
import java.util.*;


/**
 * Class that represents a hash family for {@link BitSetHashFunction}s.
 * A hash family is a set of similar hash functions.
 * 
 * @author mfranke
 *
 * @param <T>
 * 		-> the type of hash functions. The type has to imlement to {@link BitSetHashFunction} 
 * 		   interface.
 *
 * @param <U>
 * 		-> the return type of the hash functions.
 */
public class HashFamily<T extends BitSetHashFunction<U>, U> implements Serializable {

	private static final long serialVersionUID = 7730577349251809858L;
	private List<T> hashFunctions;
	
	/**
	 * Creates a new {@link HashFamily} object.
	 */
	public HashFamily(int numberOfHashFunctions){
		this.hashFunctions = new ArrayList<T>(numberOfHashFunctions);
	}
	
	/**
	 * Creates a new {@link HashFamily} object.
	 * 
	 * @param hashFunctions
	 * 		-> a {@link List} of hash functions from type T.
	 */
	public HashFamily(List<T> hashFunctions){
		this.hashFunctions = hashFunctions;
	}
	
	public List<T> getHashFunctions(){
		return this.hashFunctions;
	}
	
	public void setHashFunctions(List<T> hashFunctions){
		this.hashFunctions = hashFunctions;
	}
	
	public void addHashFunction(T hashFunction){
		this.hashFunctions.add(hashFunction);
	}
	
	public int getNumberOfHashFunctions(){
		return this.hashFunctions.size();
	}
	
	/**
	 * Calculates the hash values for all hash functions in this hash family.
	 * 
	 * @param bitset
	 * 		-> the {@link BitSet} object to hash.
	 * 
	 * @return
	 * 		-> {@link List} of hash values U.
	 */
	public List<U> calculateHashes(BitSet bitset){
		final int numberOfHashFunctions = this.hashFunctions.size();
		
		List<U> hashes = new ArrayList<U>(numberOfHashFunctions);
		
		for (int i = 0; i < numberOfHashFunctions; i++){
			final T hashFunction = this.hashFunctions.get(i);
			final U hashValue = hashFunction.hash(bitset);
			hashes.add(hashValue);
		}
		
		return hashes;
	}
	
	
	public static Set<Integer> selectRandomPositions(int numberOfHashesPerFamily, int valueRange, long seed){	
		final Random rnd = new Random(seed);
		
		final Set<Integer> randomPositions = new LinkedHashSet<Integer>(numberOfHashesPerFamily);
		
		for (int i = 0; i < numberOfHashesPerFamily; i++){
			Integer randomIndex = rnd.nextInt(valueRange);
			
			while (randomPositions.contains(randomIndex)){
				randomIndex = rnd.nextInt(valueRange);
			}
			
			randomPositions.add(randomIndex);
		}
		
		return randomPositions;
	}

	/**
	 * Generates a family of {@link IndexHash} functions with random indices.
	 * 
	 * @param numberOfHashesPerFamily
	 * 		-> count of hash functions in this hash family.
	 * 
	 * @param valueRange
	 * 		-> the range in which the positions / indices should be selected.
	 * 
	 * @return
	 * 		-> the generated index hash family.
	 */
	public static HashFamily<IndexHash, Boolean> generateRandomIndexHashFamily(
		int numberOfHashesPerFamily, int valueRange) {
			return generateRandomIndexHashFamily(numberOfHashesPerFamily, valueRange, null);
	}

	public static HashFamily<IndexHash, Boolean> generateRandomIndexHashFamily(
			int numberOfHashesPerFamily, int valueRange, List<Integer> bits) {
		final HashFamily<IndexHash, Boolean> hashFamily = 
				new HashFamily<IndexHash, Boolean>(numberOfHashesPerFamily);
		
		final Random rnd = new Random();
		final ArrayList<Integer> indices = new ArrayList<Integer>(numberOfHashesPerFamily);
		final int usefulBitCount = ((bits != null) ? bits.size() : 0); 
		
		if (usefulBitCount == 0){
			for (int i = 0; i < numberOfHashesPerFamily; i++){
				
				Integer randomIndex = rnd.nextInt(valueRange);
				
				while (indices.contains(randomIndex)){
					randomIndex = rnd.nextInt(valueRange);
				}
				
				indices.add(randomIndex);
				
				final IndexHash indexHash = new IndexHash(randomIndex);

				hashFamily.addHashFunction(indexHash);
			}
		}
		else{
			for (int i = 0; i < numberOfHashesPerFamily; i++){
				Integer randomIndex = rnd.nextInt(usefulBitCount);
				
				while(indices.contains(randomIndex)){
					randomIndex = rnd.nextInt(usefulBitCount);
				}
				
				indices.add(randomIndex);
				
				final IndexHash indexHash = new IndexHash(bits.get(randomIndex));
				
				hashFamily.addHashFunction(indexHash);
			}
		}
		return hashFamily;
	}

	public static HashFamily<IndexHash, Boolean> fromPositions(Integer[] positions) {
		final int numberOfHashesPerFamily = positions.length;
		
		final HashFamily<IndexHash, Boolean> hashFamily = 
				new HashFamily<IndexHash, Boolean>(numberOfHashesPerFamily);
		
		for (int i = 0; i < numberOfHashesPerFamily; i++){
			final IndexHash indexHash = new IndexHash(positions[i]);
			hashFamily.addHashFunction(indexHash);
		}
		
		return hashFamily;
	}
}