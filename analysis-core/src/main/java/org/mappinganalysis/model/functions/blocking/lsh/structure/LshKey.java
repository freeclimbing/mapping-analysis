package org.mappinganalysis.model.functions.blocking.lsh.structure;

import java.util.BitSet;

/**
 * Class that represents a lsh key build for a specific
 * composite hash function (i.e. hash family).
 * 
 * @author mfranke
 *
 */
public class LshKey {

	private Integer id;
	private BitSet bitset;	
	private long[] bits;
	
	/**
	 * Creates a new lsh key.
	 */
	public LshKey(){}
	
	/**
	 * Creates a new lsh key.
	 * @param id the id of the hash family to which this key corresponds.
	 * @param bitset the value of the lsh key.
	 */
	public LshKey(Integer id, BitSet bitset){
		this.id = id;
		this.bitset = bitset;
		this.bits = bitset.toLongArray();
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public BitSet getBitset() {
		return bitset;
	}

	public void setBitset(BitSet bitset) {
		this.bitset = bitset;
		this.bits = bitset.toLongArray();
	}

	public long[] getBits() {
		return bits;
	}

	public void setBits(long[] bits) {
		this.bits = bits;
	}
	

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		LshKey other = (LshKey) obj;
		if (bitset == null) {
			if (other.bitset != null) {
				return false;
			}
		}
		else if (!bitset.equals(other.bitset)) {
			return false;
		}
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} 
		else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[keyId=");
		builder.append(id);
		builder.append(", lshKey=");
		builder.append(bitset);
		builder.append("]");
		return builder.toString();
	}
}