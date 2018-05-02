package org.mappinganalysis.model.functions.preprocessing.utils;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 *  Tuple representation for an edge with contained information
 *  - cc id
 *  - source vertex id
 *  - target vertex id
 *  - starting vertex data source
 *  - neighbor vertex data source
 *  - edge similarity
 */
public class EdgeSourceSimTuple
    extends Tuple6<Long, Long, Long, String, String, Double> {
  public EdgeSourceSimTuple() { // needed
  }

  EdgeSourceSimTuple(Long ccId, Long source, Long target, String srcDatasource, String trgDataSource, Double edgeSim) {
    this.f0 = ccId;
    this.f1 = source;
    this.f2 = target;
    this.f3 = srcDatasource;
    this.f4 = trgDataSource;
    this.f5 = edgeSim;
  }

  public Long getCcId() {
    return f0;
  }

  public Long getSrcId() {
    return f1;
  }

  public Long getTrgId() {
    return f2;
  }

  String getSrcDataSource() {
    return f3;
  }

  String getTrgDataSource() {
    return f4;
  }

  public Double getLinkSim() {
    return f5;
  }
}
