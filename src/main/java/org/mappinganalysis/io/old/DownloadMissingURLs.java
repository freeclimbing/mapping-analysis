package org.mappinganalysis.io.old;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;

import org.mappinganalysis.utils.Utils;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public class DownloadMissingURLs {

	/*
	 * some concepts and their attributes have been missing due to connection timeout
	 * they are contained in file "missingURL.txt"
	 * this class downloads missing concept informations for links in the file
	 */
	
	public static void main(String[] args) throws IOException {
		
        boolean alsoSearchObsoletes = true;
        
	    BufferedReader br = new BufferedReader(new FileReader("D:/Daten/workspace/BioportalMappingAnalysis" +
	    		"/data/ontologies/missingURL.txt"));
	    String link = "";
	    String currentOnto = "missingConcepts";
	    
	    String current = new java.io.File( "." ).getCanonicalPath().replace("\\", "/").replace(" ", "%20");        
	    String dir = current+"/data/ontologies/";
	    String fileLabels = currentOnto+"_uriLabel";
	    String fileSyns = currentOnto+"_uriSyn";
	    String fileObs = currentOnto+"_uriObsStatus";	
	        
        Writer fwLabel = new FileWriter(dir+fileLabels+".txt");
        Writer fwSyns = new FileWriter(dir+fileSyns+".txt");
        Writer fwObs = new FileWriter(dir+fileObs+".txt");
	    
	    while((link = br.readLine())!=null){
	    		    	    	
	    	currentOnto = link.split("data.bioontology.org/ontologies/")[1].split("/")[0];
	        
	      /*  FileReader rLabel = new FileReader(dir+fileLabels+".txt");
	        FileReader rSyns = new FileReader(dir+fileSyns+".txt");
	        FileReader rObs = new FileReader(dir+fileObs+".txt");
	
	        Writer fwLabel = new FileWriter(dir+fileLabels+".txt");
	        Writer fwSyns = new FileWriter(dir+fileSyns+".txt");
	        Writer fwObs = new FileWriter(dir+fileObs+".txt");
	    	
	        fwLabel.append(rLabel.toString());
	        fwSyns.append(rSyns.toString());
	        fwObs.append(rObs.toString());*/
	        
	    	HttpURLConnection conn = null;
	    	JsonObject jsonObject = null;
	    	try{
				conn = Utils.openUrlConnection(new URL(link));
				BufferedReader br2 = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		        jsonObject = JsonObject.readFrom(br2);
		        
		        String label = "";
				if(!jsonObject.get("prefLabel").isNull()){
					label = jsonObject.get("prefLabel").asString();
				}else{
					System.out.println("label is null: "+jsonObject.get("@id"));
				}
				
				JsonArray synonyms = jsonObject.get("synonym").asArray();
				Boolean obsStatus; 
				if(alsoSearchObsoletes){
					obsStatus = jsonObject.get("obsolete").asBoolean();
				}else{
					obsStatus = false;
				}
				
				String uri = jsonObject.get("@id").asString();
				System.out.println(label + "\t" +synonyms + "\t" + uri);
				
				//write labels, synonyms and obsolete status per URI to csv file
				if(!uri.contains(".well-known/genid")){
					fwLabel.append(uri+"\t"+label+"\n");
					for(JsonValue syn:synonyms) fwSyns.append(uri+"\t"+syn.asString()+"\n");
					fwObs.append(uri+"\t"+obsStatus.toString()+"\n");					
				}
				conn.disconnect();
				
			}catch(Exception e){
				System.out.println(link);
				System.out.println(e);
			}
		    fwLabel.flush();		
			fwSyns.flush();
			fwObs.flush();
	    }
	    		
	    fwLabel.close();		
		fwSyns.close();
		fwObs.close();
		
		br.close();
	}
}
