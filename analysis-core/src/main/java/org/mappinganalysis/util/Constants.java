package org.mappinganalysis.util;

import org.mappinganalysis.io.impl.DataDomain;

/**
 * Constants for mapping analysis
 */
public class Constants {
  /**
   * Database property names.
   */
  public static final String DEFAULT_VALUE = "default";
  public static final String DEBUG = "debug";
  public static final String INFO = "info";
  public static final String GEO_FULL_NAME = "dbURLfull";
  public static final String DB_PROPERY_FILE_NAME = "db";
  public static final String DB_URL_FIELD = "url";

  public static final String ID = "id";
  public static final String SOURCE = "source";
  public static final String TARGET = "target";
  public static final String DATA = "data";

  /**
   * Accumulators
   */
  public static final String PROP_COUNT_ACCUMULATOR = "prop-count";
  public static final String EDGE_COUNT_ACCUMULATOR = "edge-count";
  public static final String VERTEX_COUNT_ACCUMULATOR = "vertex-count";
  public static final String BASE_VERTEX_COUNT_ACCUMULATOR = "vertex-count";
  public static final String PREPROC_LINK_FILTER_ACCUMULATOR = "links-filtered-counter";
  public static final String FILTERED_LINKS_ACCUMULATOR = "links-filtered";
  public static final String RESTRICT_EDGE_COUNT_ACCUMULATOR = "restrict-count";
  public static final String SIMSORT_EXCLUDE_FROM_COMPONENT_ACCUMULATOR = "exclude-from-component-counter";
  public static final String EDGE_EXCLUDE_ACCUMULATOR = "edge-exclude-counter";
  public static final String EXCLUDE_VERTEX_ACCUMULATOR = "exclude-vertex-counter";
  public static final String REPRESENTATIVE_ACCUMULATOR = "representative-counter";
  public static final String VERTEX_OPTIONS = "vertex-options";
  public static final String AGG_PREFIX = "aggregated-";

  /**
   * similarity default values.
   */
  public static final Double MAXIMAL_GEO_DISTANCE = 15000D;
  public static final Double SHADING_TYPE_SIM = 1D;

  /**
   * similarity computation strategies
   */
  public static final String SIM_GEO_LABEL_STRATEGY = "geo-label";
  public static final String MUSIC = "music";
  /**
   * DB attName for gn type detail information
   */
  public static final String GN_TYPE_DETAIL = "typeDetail";

  /**
   * DB column name for 'rdf:type'/..., 'geo:lat/lon', 'rdfs:label'/'gn:name'/... field.
   */
  public static final String TYPE = "type";
  public static final String LAT = "lat";
  public static final String LON = "lon";
  public static final String GEO = "geo";
  public static final String LABEL = "label";

  /**
   * DB column name for GeoNames second type value field.
   */
  public static final String TYPE_INTERN = "typeIntern";
  public static final String TMP_TYPE = "tmpType" ;
  public static final String COMP_TYPE = "compType";

  /**
   * temp values for missing values
   */
  public static final String NO_LABEL_FOUND = "no_label_found";
  public static final String NO_TYPE = "no_type";
  public static final String NO_OR_MINOR_LANG = "no_or_minor_lang";

  /**
   * Field name for sim value names
   */
  public static final String SIM_TYPE = "typeMatch";
  public static final String SIM_TRIGRAM = "trigramSim";
  public static final String SIM_DISTANCE = "distance";

  /**
   * Aggregated similarity value on edge
   */
  public static final String AGGREGATED_SIM_VALUE = "aggSimValue";

  /**
   * Aggregated similarity of all edges which are outgoing or incoming to a vertex
   */
  public static final String VERTEX_AGG_SIM_VALUE = "vertexAggSimValue";

  /**
   * SimSort status flag
   */
  public static final String VERTEX_STATUS = "vertexStatus";
  public static final Double DEFAULT_VERTEX_SIM = -1.0D;

  /**
   * field name for connected component ID
   */
  public static final String CC_ID = "ccId";
  public static final String VERTEX_ID = "vertexId";
  public static final String HASH_CC = "hashCc";
  public static final String OLD_HASH_CC = "oldHashCc";
  public static final String REFINE_ID = "refineId";

  /**
   * property name in cluster representative where clustered vertivces are put into.
   */
  public static final String CL_VERTICES = "clusteredVertices";

  /**
   * source name of resource. internal name for db field ontID_fk
   */
  public static final String DATA_SOURCE = "ontology";
  public static final String DATA_SOURCES = "ontologies";

  /**
   * Music vocabulary
   */
  public static final String LANGUAGE = "language";
  public static final String LENGTH = "length";
  public static final String YEAR = "year";
  public static final String ARTIST = "artist";
  public static final String ALBUM = "album";
  public static final String NUMBER = "number";

  /**
   * source namespaces
   */
  public static final String DBP_NS = "http://dbpedia.org/";
  public static final String GN_NS = "http://sws.geonames.org/";
  public static final String LGD_NS = "http://linkedgeodata.org/";
  public static final String FB_NS = "http://rdf.freebase.com/";
  public static final String NYT_NS = "http://data.nytimes.com/";

  /**
   * geo rdf:types condensed to some key values
   */
  public static final String AS = "ArchitecturalStructure";
  public static final String M = "Mountain";
  public static final String AR = "AdministrativeRegion";
  public static final String B = "BodyOfWater";
  public static final String P = "Park";
  public static final String S = "Settlement";

  /**
   * General vocabulary to read input and run jobs
   */
  public static final String INPUT_GRAPH = "InputGraph";
  public static final String PREPROC_GRAPH = "PreprocGraph";
  public static final String DECOMP_GRAPH = "DecompGraph";
  public static final String LF_GRAPH = "LfGraph";
  public static final String IC_GRAPH = "IcGraph";
  public static final String SIMSORT_GRAPH = "SimSortGraph";

  public static final String READ_INPUT = "read-input";
  public static final String PREPROC = "preproc";
  public static final String LF = "lf";
  public static final String PREPROCESSING_COMPLETE = "pc";
  public static final String IC = "ic";
  public static final String DECOMP = "decomp";
  public static final String SIMSORT = "simsort";
  public static final String DECOMPOSITION_COMPLETE = "dc";
  public static final String ANALYSIS = "analysis";
  public static final String ALL = "all"; // all parts in a row, multiple jobs
  public static final String COMPLETE = "c"; // all parts together
  public static final String EVAL = "eval";
  public static final String STATS_EDGE_INPUT = "stats-edge-input";
  public static final String STATS_EDGE_PREPROC = "stats-edge-preproc";
  public static final String STATS_VERTEX_INPUT = "stats-vertex-input";
  public static final String STATS_VERTEX_PREPROC = "stats-vertex-preproc";
  public static final String INIT_CLUST = "2-initial-clustering";
  public static final String MISS = "miss";
  public static final String TEST = "test";

  /**
   * Command line option names.
   */
  public static String INPUT_DIR;

  public static final String VERTICES = "vertices/";
  public static final String EDGES = "edges/";
  public static final String OUTPUT = "output/";
  public static final String INPUT = "input/";
  public static final String SLASH = "/";

  /**
   * Number of different data sources in the data set.
   */
  public static Integer SOURCE_COUNT;
  public static String LL_MODE = "";
  public static String PROC_MODE = "";
  public static String VERBOSITY;
  public static DataDomain DOMAIN;
  public static boolean IS_SIMSORT_ENABLED;
}
