package org.mappinganalysis.model.functions.preprocessing.utils;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 *  Tuple representation for an edge with contained information
 *  - source vertex id
 *  - target vertex id
 *  - starting vertex data source (ontology)
 *  - neighbor vertex data source (ontology)
 *  - edge similarity
 */
public class EdgeSourceSimTuple
    extends Tuple6<Long, Long, Long, String, String, Double> {

  public EdgeSourceSimTuple() {
  }

  public EdgeSourceSimTuple(Long ccId, Long source, Long target, String srcOntology, String trgOntology, Double edgeSim) {
    this.f0 = ccId;
    this.f1 = source;
    this.f2 = target;
    this.f3 = srcOntology;
    this.f4 = trgOntology;
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

  public String getSrcOntology() {
    return f3;
  }

  public String getTrgOntology() {
    return f4;
  }

  public Double getLinkSim() {
    return f5;
  }

}
