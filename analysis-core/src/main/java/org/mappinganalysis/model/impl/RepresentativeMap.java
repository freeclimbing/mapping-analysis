package org.mappinganalysis.model.impl;

import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;

import java.io.Serializable;

public class RepresentativeMap extends ObjectMap implements Serializable {
  private static final long serialVersionUID = 42L;

  private static final Logger LOG = Logger.getLogger(RepresentativeMap.class);

  public RepresentativeMap(ObjectMap properties, DataDomain domain) {
    super(properties, domain);
  }

  public RepresentativeMap(DataDomain domain) {
    super(domain);
  }

//  @Deprecated
//  public void setClusterDataSources(Set<String> sources) {
//    if (!sources.isEmpty()) {
//      this.put(Constants.DATA_SOURCES, sources);
//    }
//  }
//
//  /**
//   * Get internal representation of all data sources in a cluster.
//   */
//  @Deprecated
//  public Integer getIntDataSources() {
//    Object dataSources = this.get(Constants.DATA_SOURCES);
//    Set<String> sources;
//    if (dataSources instanceof Set) {
//      sources = (Set<String>) dataSources;
//    } else if (dataSources == null) {
//      sources = Sets.newHashSet(getDataSource());
//    } else {
//      sources = Sets.newHashSet(dataSources.toString());
//    }
//
//    return AbstractionUtils.getSourcesInt(this.getMode(), sources);
//  }
}
