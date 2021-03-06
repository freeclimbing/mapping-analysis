package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple9;
import org.mappinganalysis.model.api.ClusteredGeoEntity;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

import java.util.Set;

/**
 * MergeTuple consists of
 * 0. vertex id
 * 1. label
 * 2. lat
 * 3. lon
 * 4. type (as int)
 * 5. sources (as int)
 * 6. clustered elements list
 * 7. blocking label
 * 8. activity flag
 */
public class MergeGeoTuple
    extends Tuple9<Long, String, Double, Double, Integer, Integer, LongSet, String, Boolean>
    implements ClusteredGeoEntity, MergeTupleAttributes {

  public MergeGeoTuple() {
    this.f6 = new LongSet();
    this.f2 = 1000D;
    this.f3 = 1000D;
    this.f8 = true;
  }

  /**
   * Constructor for fake tuples (with fake values)
   */
  public MergeGeoTuple(Long id) {
    super(id,
        Constants.EMPTY_STRING,
        1000D,
        1000D,
        0,
        0,
        new LongSet(id),
        Constants.EMPTY_STRING,
        false);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(getId()).append(Constants.COMMA);
    stringBuilder.append(getLabel()).append(Constants.COMMA);
    stringBuilder.append(getLatitude()).append(Constants.COMMA);
    stringBuilder.append(getLongitude()).append(Constants.COMMA);
    stringBuilder.append(getIntTypes()).append(Constants.COMMA);
    stringBuilder.append(AbstractionUtils
        .getSourcesStringSet(Constants.GEO, getIntSources()))
        .append(Constants.COMMA);
    stringBuilder.append(getClusteredElements()).append(Constants.COMMA);
    stringBuilder.append(getBlockingLabel()).append(Constants.COMMA);
    stringBuilder.append(isActive()).append(Constants.COMMA);

    return stringBuilder.toString();
  }

  @Override
  public Long getId() {
    return f0;
  }

  @Override
  public void setId(Long vertexId) {
    f0 = vertexId;
  }

  @Override
  public String getLabel() {
    return f1;
  }

  @Override
  public void setLabel(String label) {
    f1 = label;
  }

  @Override
  public Double getLatitude() {
    return f2;
  }

  @Override
  public Double getLongitude() {
    return f3;
  }

  public void setGeoProperties(MergeGeoTuple input) {
    f2 = input.getLatitude();
    f3 = input.getLongitude();
  }

  @Override
  public void setLatitude(Double latitude) {
    f2 = latitude;
  }

  @Override
  public void setLongitude(Double longitude) {
    f3 = longitude;
  }

  @Override
  public Integer getIntTypes() {
    return f4;
  }

  @Override
  public void setIntTypes(Integer types) {
    f4 = types;
  }

  @Override
  public Integer getIntSources() {
    return f5;
  }

  @Override
  public void setIntSources(Integer intSources) {
    f5 = intSources;
  }

  @Override
  public Set<Long> getClusteredElements() {
    return f6;
  }

  @Override
  public void addClusteredElements(Set<Long> elements) {
    f6.addAll(elements);
  }

  @Override
  public Integer size() {
    return AbstractionUtils.getSourceCount(f5);
  }

  public void setBlockingLabel(String label) {
    f7 = label;
  }

  public String getBlockingLabel() {
    return f7;
  }

  @Override
  public boolean isActive() {
    return f8;
  }

  @Override
  public void setActive(Boolean value) {
    f8 = value;
  }

}
