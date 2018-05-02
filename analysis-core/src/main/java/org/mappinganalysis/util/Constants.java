package org.mappinganalysis.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Constants for mapping analysis
 */
public class Constants {
  /**
   * Database property names.
   */
  public static final String DEBUG = "debug";
  public static final String GEO_FULL_NAME = "dbURLfull";
  public static final String DB_PROPERY_FILE_NAME = "db";
  public static final String DB_URL_FIELD = "url";

  public static final String ID = "id";
  public static final String GRADOOP_ID = "gradoopId";
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
  public static final String REPRESENTATIVE_ACCUMULATOR = "representative-counter";

  /*
    Incremental strategies
   */
  public static final String FIXED = "fixed";
  public static final String BIG = "big";
  public static final String SINGLE_SETTING = "single-setting";
  public static final String SPLIT_SETTING = "split-setting";

  public static final String JARO_WINKLER = "jw";
  public static final String COSINE_TRIGRAM = "ct";
  /**
   * similarity default values.
   * 150000m default distance
   */
  public static final Double MAXIMAL_GEO_DISTANCE = 150000D;
  public static final Double SHADING_TYPE_SIM = 1D;

  public static final String DATASET = "dataset";
  public static final String PM_PATH = "pmPath";
  public static final String MERGE_THRESHOLD = "mergeThreshold";
  public static final String SIMSORT_THRESHOLD = "simsortThreshold";
  public static final String SOURCE_COUNT_LABEL = "sourceCountLabel";
  public static final String DATA_SOURCES_LABEL = "dataSourcesLabel";
  public static final String BLOCKING_STRATEGY = "blockingStrategy";
  public static final String INCREMENTAL_STRATEGY = "incrementalStrategy";
  public static final String DATA_DOMAIN = "dataDomain";
  @Deprecated
  public static final String MODE = "mode";
  public static final String NEW_SOURCE = "newSource";
  public static final String METRIC = "metric";
  public static final String ENV = "env";
  public static final String IS_INCREMENTAL = "isIncremental";

  /**
   * blocking strategies
   */
  public static final String SB = "SB";
  public static final String BS = "BS";
  public static final String LSHB = "LSHB";

  /**
   * mode for MUSIC domain
   */
  public static final String MUSIC = "music";
  /**
   * mode for NC domain
   */
  public static final String NC = "northCarolina";
  /**
   * mode for GEOGRAPHY domain
   */
  public static final String GEO = "geo";
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
  public static final String LABEL = "label";
  public static final String IDF_LABEL = "idfLabel";


  /**
   * DB column name for GeoNames second type value field.
   */
  public static final String TYPE_INTERN = "typeIntern";
  public static final String COMP_TYPE = "compType";

  /**
   * temp values for missing values
   */
  public static final String NO_LABEL_FOUND = "no_label_found";
  public static final String NO_TYPE = "no_type";
  public static final String NO_VALUE = "no_value";
  public static final String CSV_NO_VALUE = "--";
  public static final String EMPTY_STRING = "";
  public static final String WHITE_SPACE = " ";
  public static final String COMMA = ",";
  public static final String DEVIDER = " - ";
  public static final int EMPTY_INT = 0;

  /**
   * Field name for sim value names
   */
  public static final String SIM_TYPE = "simType";
  public static final String SIM_LABEL = "simLabel";
  public static final String SIM_DISTANCE = "simDistance";

  public static final String SIM_ARTIST = "simArtist";
  public static final String SIM_ALBUM = "simAlbum";
  public static final String SIM_LANG = "simLang";
  public static final String SIM_YEAR = "simYear";
  public static final String SIM_LENGTH = "simLength";
  public static final String SIM_NUMBER = "simNumber";

  public static final HashSet<String> SIM_VALUES;
  public static final String SIM_ARTIST_LABEL_ALBUM = "simArtistLabelAlbum";

  static {
    SIM_VALUES = Sets.newHashSet();
    SIM_VALUES.add(SIM_TYPE);
    SIM_VALUES.add(SIM_LABEL);
    SIM_VALUES.add(SIM_DISTANCE);
    SIM_VALUES.add(SIM_ARTIST);
    SIM_VALUES.add(SIM_ALBUM);
    SIM_VALUES.add(SIM_LANG);
    SIM_VALUES.add(SIM_YEAR);
    SIM_VALUES.add(SIM_LENGTH);
    SIM_VALUES.add(SIM_NUMBER);
  }


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
  public static final String ONTOLOGY = "ontology";
  public static final String DATA_SOURCE = "dataSource";
  public static final String DATA_SOURCES = "dataSources";
  public static final String DS_COUNT = "ds-count";

  public static final String REC_ID = "recId";
  public static final String CLS_ID = "clsId";

  /**
   * Music vocabulary
   */
  public static final String TITLE = "title";
  public static final String LANGUAGE = "language";
  public static final String LENGTH = "length";
  public static final String YEAR = "year";
  public static final String ARTIST = "artist";
  public static final String ALBUM = "album";
  public static final String NUMBER = "number";
  public static final String BLOCKING_LABEL = "blockingLabel";
  public static final String ARTIST_TITLE_ALBUM = "artistTitleAlbum";
  public static final String IDF_VALUES = "idfValues";

  /**
   * NC vocabulary
   */
  public static final String SUBURB = "suburb";
  public static final String NAME = "name";
  public static final String SURNAME = "surname";
  public static final String POSTCOD = "postcod";

  /**
   * Language vocabulary
   */
  public static final String NO_OR_MINOR_LANG = "no_or_minor_lang";
  public static final String UN = "unknown";
  public static final String MU = "multiple";

  public static final String GE = "german";
  public static final String EN = "english";
  public static final String SP = "spanish";
  public static final String IT = "italian";
  public static final String FR = "french";
  public static final String LA = "latin";
  public static final String HU = "hungarian";
  public static final String PO = "polish";
  public static final String CH = "chinese";
  public static final String CA = "catalan";
  public static final String GR = "greek";
  public static final String NO = "norwegian";
  public static final String ES = "esperanto";
  public static final String POR = "portuguese";
  public static final String FI = "finnish";
  public static final String JA = "japanese";
  public static final String SW = "swedish";
  public static final String DU = "dutch";
  public static final String RU = "russian";
  public static final String TU = "turkish";
  public static final String DA = "danish";

  /**
   * source namespaces
   */
  public static final String DBP_NS = "http://dbpedia.org/";
  public static final String GN_NS = "http://sws.geonames.org/";
  public static final String LGD_NS = "http://linkedgeodata.org/";
  public static final String FB_NS = "http://rdf.freebase.com/";
  public static final String NYT_NS = "http://data.nytimes.com/";

  public static final String NC_1 = "geco1";
  public static final String NC_2 = "geco2";
  public static final String NC_3 = "geco3";
  public static final String NC_4 = "geco4";
  public static final String NC_5 = "geco5";
  public static final String NC_6 = "geco6";
  public static final String NC_7 = "geco7";
  public static final String NC_8 = "geco8";
  public static final String NC_9 = "geco9";
  public static final String NC_10 = "geco10";

  public static final List<String> GEO_SOURCES = Lists
      .newArrayList(DBP_NS, GN_NS, LGD_NS, FB_NS, NYT_NS);
  public static final List<String> MUSIC_SOURCES = Lists
      .newArrayList("1", "2", "3", "4", "5");
  public static final List<String> NC_SOURCES = Lists.newArrayList(
      NC_1, NC_2, NC_3, NC_4, NC_5, NC_6,  NC_7,  NC_8, NC_9,  NC_10);

  /**
   * TODO create these maps on-the-fly #124
   */
  public static final HashMap<String, Integer> GEO_MAP;
  static {
    GEO_MAP = Maps.newLinkedHashMap();
    GEO_MAP.put(Constants.NYT_NS, 1);
    GEO_MAP.put(Constants.DBP_NS, 2);
    GEO_MAP.put(Constants.LGD_NS, 4);
    GEO_MAP.put(Constants.FB_NS, 8);
    GEO_MAP.put(Constants.GN_NS, 16);
  }

  public static final HashMap<String, Integer> MUSIC_MAP;
  static {
    MUSIC_MAP = Maps.newLinkedHashMap();
    MUSIC_MAP.put("1", 1);
    MUSIC_MAP.put("2", 2);
    MUSIC_MAP.put("3", 4);
    MUSIC_MAP.put("4", 8);
    MUSIC_MAP.put("5", 16);
  }

  public static final HashMap<String, Integer> NC_MAP;
  static {
    NC_MAP = Maps.newLinkedHashMap();
    NC_MAP.put("geco1", 1);
    NC_MAP.put("geco2", 2);
    NC_MAP.put("geco3", 4);
    NC_MAP.put("geco4", 8);
    NC_MAP.put("geco5", 16);
    NC_MAP.put("geco6", 32);
    NC_MAP.put("geco7", 64);
    NC_MAP.put("geco8", 128);
    NC_MAP.put("geco9", 256);
    NC_MAP.put("geco10", 512);

  }

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
  public static String INPUT_PATH;

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
  public static boolean IS_SIMSORT_ENABLED;
}
