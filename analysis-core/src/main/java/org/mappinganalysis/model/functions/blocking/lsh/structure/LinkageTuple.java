package org.mappinganalysis.model.functions.blocking.lsh.structure;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.BitSet;

/**
 * Basic input object for the pprl flink task.
 *
 * @author mfranke
 */
public class LinkageTuple extends Tuple2<Long, BloomFilter> {
	public LinkageTuple() {
	}

	/**
	 * Creates a new {@link LinkageTuple}.
	 * @param id the id of the person.
	 * @param bloomFilter the bloom filter related to the person.
	 */
	public LinkageTuple(Long id, BloomFilter bloomFilter) {
		this.f0 = id;
		this.f1 = bloomFilter;
	}

	/**
	 * Parses an Tuple2<Long,BitSet> into a {@link LinkageTuple}.
	 * @param inputTuple the Tuple2<Long, BitSet>, probably a result of reading a csv file.
	 * @return the resulting {@link LinkageTuple}.
	 */
	@Deprecated
	public static LinkageTuple from(Tuple2<Long, BitSet> inputTuple) {
		LinkageTuple linkageTuple = new LinkageTuple();
		linkageTuple.setId(inputTuple.f0);
		BloomFilter bf = new BloomFilter(inputTuple.f1);
		linkageTuple.setBloomFilter(bf);

		return linkageTuple;
	}

	/**
	 * Checks if the id of two {@link LinkageTuple}s is equal.
	 * @param other an other {@link LinkageTuple}.
	 * @return boolean flag specifying whether or not the ids are equal.
	 */
	public boolean idEquals(LinkageTuple other){
		return f0.equals(other.getId());
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[id=");
		builder.append(f0);
		builder.append(", bF=");
		builder.append(f1);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((f0 == null) ? 0 : f0.hashCode());
		return result;
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
		LinkageTuple other = (LinkageTuple) obj;

		return f0 == other.f0.longValue();
	}

	public Long getId() {
		return f0;
	}

	public void setId(Long id) {
		this.f0 = id;
	}

	public BloomFilter getBloomFilter() {
		return f1;
	}

	public void setBloomFilter(BloomFilter bloomFilter) {
		this.f1 = bloomFilter;
	}
}