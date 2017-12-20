//package org.mappinganalysis.model.functions.blocking.lsh.utils;
//
///**
// * Basic input object for the pprl flink task.
// * A linkage tuple is the representation of a coded and anonymized
// * person record containing an id to identify the related person,
// * a data set identification to identify to which data source the person
// * is related and the bloom filter containing the qid values of the person.
// *
// * @author mfranke
// *
// */
//public class LinkageTuple {
//
//	protected String id;
//	protected String dataSetId;
//	protected BloomFilter bloomFilter;
//
//	/**
//	 * Empty default constructor.
//	 */
//	public LinkageTuple(){
//		this(null, null, null);
//	}
//
//	/**
//	 * Creates a new {@link LinkageTuple}.
//	 * @param id the id of the person.
//	 * @param dataSetId the id of the data set the person is related to.
//	 * @param bloomFilter the bloom filter related to the person.
//	 */
//	public LinkageTuple(String id, String dataSetId, BloomFilter bloomFilter){
//		this.id = id;
//		this.dataSetId = dataSetId;
//		this.bloomFilter = bloomFilter;
//	}
//
//	/**
//	 * Parses an {@link InputTuple} into a {@link LinkageTuple}.
//	 * @param inputTuple the {@link InputTuple}, probably a result of reading a csv file.
//	 * @return the resulting {@link LinkageTuple}.
//	 */
//	public static LinkageTuple from(InputTuple inputTuple){
//		LinkageTuple linkageTuple = new LinkageTuple();
//		linkageTuple.setId(inputTuple.getId());
//		linkageTuple.setDataSetId(inputTuple.getDataSetId());
//		BloomFilter bf = BloomFilter.from(inputTuple.getBloomFilterBits());
//		linkageTuple.setBloomFilter(bf);
//
//		return linkageTuple;
//	}
//
//	/**
//	 * Checks if the id of two {@link LinkageTuple}s is equal.
//	 * @param other an other {@link LinkageTuple}.
//	 * @return boolean flag specifying whether or not the ids are equal.
//	 */
//	public boolean idEquals(LinkageTuple other){
//		return this.id.equals(other.getId());
//	}
//
//	/**
//	 * Checks if the data set ids of two {@link LinkageTuple}s is equal.
//	 * @param other an other {@link LinkageTuple}.
//	 * @return boolean flag specifying whether or not the data set ids are equal.
//	 */
//	public boolean dataSetIdEquals(LinkageTuple other){
//		return this.dataSetId.equals(other.getDataSetId());
//	}
//
//	@Override
//	public String toString() {
//		StringBuilder builder = new StringBuilder();
//		builder.append("[id=");
//		builder.append(id);
//		builder.append(", dId=");
//		builder.append(dataSetId);
//		builder.append(", bF=");
//		builder.append(bloomFilter);
//		builder.append("]");
//		return builder.toString();
//	}
//
//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + ((dataSetId == null) ? 0 : dataSetId.hashCode());
//		result = prime * result + ((id == null) ? 0 : id.hashCode());
//		return result;
//	}
//
//	@Override
//	public boolean equals(Object obj) {
//		if (this == obj) {
//			return true;
//		}
//		if (obj == null) {
//			return false;
//		}
//		if (getClass() != obj.getClass()) {
//			return false;
//		}
//		LinkageTuple other = (LinkageTuple) obj;
//		if (dataSetId == null) {
//			if (other.dataSetId != null) {
//				return false;
//			}
//		}
//		else if (!dataSetId.equals(other.dataSetId)) {
//			return false;
//		}
//		if (id == null) {
//			if (other.id != null) {
//				return false;
//			}
//		}
//		else if (!id.equals(other.id)) {
//			return false;
//		}
//		return true;
//	}
//
//	public String getId() {
//		return id;
//	}
//
//	public void setId(String id) {
//		this.id = id;
//	}
//
//	public String getDataSetId() {
//		return dataSetId;
//	}
//
//	public void setDataSetId(String dataSetId) {
//		this.dataSetId = dataSetId;
//	}
//
//	public BloomFilter getBloomFilter() {
//		return bloomFilter;
//	}
//
//	public void setBloomFilter(BloomFilter bloomFilter) {
//		this.bloomFilter = bloomFilter;
//	}
//}