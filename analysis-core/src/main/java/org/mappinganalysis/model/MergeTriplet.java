package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.runtime.util.LongArrayList;
import org.mappinganalysis.model.api.*;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;
import org.mappinganalysis.util.Utils;

import java.math.BigDecimal;
import java.util.Set;

/**
 * MergeTriplet consists of
 * 0. vertex id
 * 1. label
 * 2. lat
 * 3. lon
 * 4. type (as int)
 * 5. sources (as int)
 * 6. clustered elements list
 * 7. similarity
 * 8. blocking label
 */
public class MergeTriplet
    extends Tuple9<Long, String, Double, Double, Integer, Integer, LongArrayList, Double, String>
    implements ClusteredEntity {
  public MergeTriplet() {
  }

//  public void setGeoCoordinates(Double latLeft, Double longLeft, Double latRight, Double longRight) {
//    if (Utils.isValidGeoObject(latLeft, longLeft)
//        && !Utils.isValidGeoObject(latRight, longRight)) {
//      f2 = latLeft;
//      f3 = longLeft;
//    } else if (!Utils.isValidGeoObject(latLeft, longLeft)
//        && Utils.isValidGeoObject(latRight, longRight)) {
//      f2 = latRight;
//      f3 = longRight;
//    } else {
//      Double distance = GeoDistance.distance(latLeft, longLeft, latRight, longRight);
//
//      if (distance >= Constants.MAXIMAL_GEO_DISTANCE) {
//        return 0D;
//      } else {
//        BigDecimal tmpResult;
//        double tmp = 1D - (distance / Constants.MAXIMAL_GEO_DISTANCE);
//        tmpResult = new BigDecimal(tmp);
//
//        return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
//      }
//    }
//  }

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

  public LongArrayList getClusteredElements() {
    return f6;
  }

  public void addClusteredElements(Set<Long> elements) {
    for (Long element : elements) {
      f6.add(element);
    }
  }

  public Integer size() {
    return AbstractionUtils.getSourceCount(f5);
  }

  public void setBlockingLabel(String label) {
    f8 = label;
  }

  public String getBlockingLabel() {
    return f8;
  }
}
