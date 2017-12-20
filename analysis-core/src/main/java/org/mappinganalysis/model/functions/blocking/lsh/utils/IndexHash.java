package org.mappinganalysis.model.functions.blocking.lsh.utils;

import java.util.BitSet;

/**
 * Class that implements a index hash function.
 * A index hash function takes a bit from a BitSet and
 * returns the boolean value of the bit.
 * 
 * @author mfranke
 *
 */
public class IndexHash implements BitSetHashFunction<Boolean> {

	private static final long serialVersionUID = -6736682672413137129L;
	private int position;
	
	/**
	 * Creates a new {@link IndexHash} object.
	 * 
	 * @param position
	 * 		-> the position of the {@link BitSet} to return the boolean value for.
	 */
	public IndexHash(int position){
		this.position = position;
	}
	
	/**
	 * Takes the {@link BitSet} object and calculates the boolean hash value
	 * by evaluate the bit at the defined position.
	 */
	@Override
	public Boolean hash(BitSet bitset) {
		return bitset.get(this.position);
	}

	public int getPosition() {
		return this.position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

}