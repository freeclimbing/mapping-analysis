package org.mappinganalysis.io;

import java.io.IOException;

public class DownloadBioportalOntologies {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JSONOntologyLoaderBioportal l = new JSONOntologyLoaderBioportal();
		String apikey = "df344784-0c8c-49c2-8a63-6067039711cd";
		
		String[] ontos = {"CHEBI","NCIT","DOID","LOINC","MESH","RXNORM","RADLEX","GALEN","OMIM","FMA","MA","PDQ","NATPRO"};//"NCIT"
		
		//for(int i = 0; i<ontos.length;i++){
			//String ontoShortName = ontos[i];
			String ontoShortName = "MA"; //
			l.getOntology(ontoShortName,apikey);
		//}
	}	
}
