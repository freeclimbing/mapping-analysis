//package org.mappinganalysis.model.functions.blocking.lsh.utils;
//
//import org.mappinganalysis.model.functions.blocking.lsh.LshKey;
//
///**
// * Special case of a {@link LinkageTuple} containing additionally
// * a array of {@link LshKey}s.
// *
// * @author mfranke
// *
// */
//public class LinkageTupleWithLshKeys extends LinkageTuple {
//
//	private LshKey[] lshKeys;
//
//	/**
//	 * Creates a new {@link LinkageTupleWithLshKeys} object.
//	 */
//	public LinkageTupleWithLshKeys(){
//		super();
//		this.lshKeys = null;
//	}
//
//	/**
//	 * Creates a new {@link LinkageTupleWithLshKeys} object.
//	 * @param linkageTuple the regular {@link LinkageTuple}.
//	 * @param lshKeys array of {@link LshKey}s.
//	 */
//	public LinkageTupleWithLshKeys(LinkageTuple linkageTuple, LshKey[] lshKeys){
//		this(linkageTuple.getId(), linkageTuple.getDataSetId(), linkageTuple.getBloomFilter(), lshKeys);
//	}
//
//	/**
//	 * Convenience constructor that creates a new object by building a
//	 * {@link LinkageTuple} by its components.
//	 *
//	 * @param id the id of the {@link LinkageTuple}.
//	 * @param dataSetId the dataset identifier for the {@link LinkageTuple}.
//	 * @param bloomFilter the bloom filter of the {@link LinkageTuple}.
//	 * @param lshKeys array of {@link LshKey}s.
//	 */
//	public LinkageTupleWithLshKeys(String id, String dataSetId, BloomFilter bloomFilter, LshKey[] lshKeys){
//		super(id, dataSetId, bloomFilter);
//		this.lshKeys = lshKeys;
//	}
//
//	public void setLinkageTuple(LinkageTuple linkageTuple){
//		this.id = linkageTuple.getId();
//		this.dataSetId = linkageTuple.getDataSetId();
//		this.bloomFilter = linkageTuple.getBloomFilter();
//	}
//
//	/**
//	 * Returns the {@link LshKey} at a specific position.
//	 * @param position the index of the {@link LshKey}.
//	 * @return the {@link LshKey}.
//	 */
//	public LshKey getLshKeyAt(int position){
//		return this.lshKeys[position];
//	}
//
//	public LshKey[] getLshKeys() {
//		return lshKeys;
//	}
//
//	public void setLshKeys(LshKey[] lshKeys) {
//		this.lshKeys = lshKeys;
//	}
//}