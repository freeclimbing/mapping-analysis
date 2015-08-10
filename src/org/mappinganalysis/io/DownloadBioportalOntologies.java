package org.mappinganalysis.io;

import java.io.IOException;

public class DownloadBioportalOntologies {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	JSONOntologyLoaderBioportal l = new JSONOntologyLoaderBioportal();
	
	String ontoShortName = "MA";
	String apikey = "df344784-0c8c-49c2-8a63-6067039711cd";
	l.getOntology(ontoShortName,apikey);

	}

}
