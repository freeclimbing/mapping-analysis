package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeMusicTuple;

/**
 * Return only active vertices.
 */
class ActiveFilterFunction<T>
    implements FilterFunction<T> {
  private DataDomain domain;

  public ActiveFilterFunction(DataDomain domain) {
    this.domain = domain;
  }

  @Override
  public boolean filter(T value) throws Exception {
    if (domain == DataDomain.GEOGRAPHY) {
      MergeGeoTuple tuple = (MergeGeoTuple) value;
      return tuple.isActive();
    } else {
      MergeMusicTuple tuple = (MergeMusicTuple) value;
      return tuple.isActive();
    }
  }
}
