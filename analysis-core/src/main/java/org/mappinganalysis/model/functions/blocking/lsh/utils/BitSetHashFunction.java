package org.mappinganalysis.model.functions.blocking.lsh.utils;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Interface for a BitSetHashfunction. Such a hash function accepts a {@link BitSet}
 * as input and returns T.
 * 
 * @author mfranke
 *
 */
public interface BitSetHashFunction<T> extends Serializable{

	/**
	 * Hashes a {@link BitSet}.
	 * 
	 * @param bitset
	 * 		-> the {@link BitSet} to hash.
	 * 
	 * @return
	 * 		-> T.
	 */
	public T hash(BitSet bitset);
}
