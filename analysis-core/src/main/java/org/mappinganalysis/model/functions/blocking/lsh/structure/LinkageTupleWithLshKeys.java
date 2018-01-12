package org.mappinganalysis.model.functions.blocking.lsh.structure;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Special case of a {@link LinkageTuple} containing additionally
 * a array of {@link LshKey}s.
 *
 * @author mfranke
 *
 */
public class LinkageTupleWithLshKeys extends Tuple3<Long, BloomFilter, LshKey[]> {
	public LinkageTupleWithLshKeys(){
	  this.f2 = new LshKey[]{};
	}

	/**
	 * Creates a new {@link LinkageTupleWithLshKeys} object.
	 * @param linkageTuple the regular {@link LinkageTuple}.
	 * @param lshKeys array of {@link LshKey}s.
	 */
	public LinkageTupleWithLshKeys(LinkageTuple linkageTuple, LshKey[] lshKeys){
	  this.f0 = linkageTuple.getId();
	  this.f1 = linkageTuple.getBloomFilter();
	  this.f2 = lshKeys;
	}

	/**
	 * Convenience constructor that creates a new object by building a
	 * {@link LinkageTuple} by its components.
	 *
	 * @param id the id of the {@link LinkageTuple}.
	 * @param bloomFilter the bloom filter of the {@link LinkageTuple}.
	 * @param lshKeys array of {@link LshKey}s.
	 */
	public LinkageTupleWithLshKeys(Long id, BloomFilter bloomFilter, LshKey[] lshKeys){
	  super(id, bloomFilter, lshKeys);
	}

	public void setLinkageTuple(LinkageTuple linkageTuple){
		this.f0 = linkageTuple.getId();
		this.f1 = linkageTuple.getBloomFilter();
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

  public void setId(BloomFilter bloomFilter) {
    this.f1 = bloomFilter;
  }

	/**
	 * Returns the {@link LshKey} at a specific position.
	 * @param position the index of the {@link LshKey}.
	 * @return the {@link LshKey}.
	 */
	public LshKey getLshKeyAt(int position){
		return this.f2[position];
	}

	public LshKey[] getLshKeys() {
		return f2;
	}

	public void setLshKeys(LshKey[] lshKeys) {
		this.f2 = lshKeys;
	}
}