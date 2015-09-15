package org.mappinganalysis.io;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.mappinganalysis.utils.Utils;

public class HTMLMappingLoaderBioportal {

	
	public HTMLMappingLoaderBioportal() {
		// TODO Auto-generated constructor stub
	}

	public void parseSubPages(String basicLink,String sourceOntoShortname, String targetOntoShortname, String apikey) throws IOException {
		String current = new java.io.File( "." ).getCanonicalPath().replace("\\", "/").replace(" ", "%20");        
        String dir = current+"/data/mappings/";
		
		String pageLink = "/mappings/"+sourceOntoShortname+"?target=http://data.bioontology.org/ontologies/"+targetOntoShortname+"&apikey="+apikey;
		
		//first page
		System.out.println("\n##################\n"+basicLink+pageLink);
	
		
		HttpURLConnection conn = Utils.openUrlConnection(new URL(basicLink+pageLink));
		Document doc = Jsoup.parse(conn.getInputStream(), "UTF-8", "");
		
		//System.out.println(doc.toString());
		
        Elements thead = doc.select("thead");
        String srcOnto ="";
        String trgOnto="";
        
        // get ontology names from table header
        for(Element th:thead){
        	srcOnto = th.getElementsByIndexEquals(0).last().text();
        	trgOnto = th.getElementsByIndexEquals(1).last().text();
        }
        System.out.println("Download BioPortal Mapping: " +srcOnto+"_"+trgOnto);
        System.out.println("from: "+pageLink);
		
        // create file
        String fileName = srcOnto+"["+sourceOntoShortname+"]"+"_"+trgOnto+"["+targetOntoShortname+"].txt"; // besetzen mit ontomap name
		Writer fw = new FileWriter(dir+fileName);
		System.out.println(dir+fileName);
		fw.append("Mapping: "+srcOnto+"["+sourceOntoShortname+"]"+"_"+trgOnto+"["+targetOntoShortname+"]\n");
		fw.append("domainURL\trangeURL\tmethod\n");
		
		//get correspondence from table body rows
		Elements rows = doc.select("tbody").select("tr");
     	
        for(Element r:rows){
        	Elements tds = r.getElementsByTag("td");
        	String tupel = "";
        	for(Element td:tds){
        		tupel +="\t"+td.text();
        	}
        	fw.append(tupel.replaceFirst("\t", "")+"\n");
        }
        
        if(!doc.getElementsByClass("next_page").isEmpty()){
        	Element link = doc.getElementsByClass("next_page").select("a").first();

        	String nextPageLink = link.attr("href");
        	parseFollowingPages(basicLink, nextPageLink, conn, fw);
    	}else{
    		System.out.println("Abbruch - CHECK what happened");
    		Element link = doc.getElementsByClass("next_page").select("a").first();
    		System.out.println(link);
    	}
        
        fw.flush();
		fw.close(); 
		conn.disconnect();
	}
	
	private static void parseFollowingPages(String basicLink, String nextPageLink, HttpURLConnection conn, Writer fw) {
		String nextPageNumber = nextPageLink.split("page=")[1].split("&")[0];
		System.out.println("Next page = "+nextPageNumber);
		
		conn.disconnect();
        try {
			conn = Utils.openUrlConnection(new URL(basicLink+nextPageLink));
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		}
    	
		Document doc = null;
		try {
			doc = Jsoup.parse(conn.getInputStream(), "UTF-8", "");
		
	        //get correspondence from table body rows
			Elements rows = doc.select("tbody").select("tr");
	     	
        	for(Element r:rows){
            	Elements tds = r.getElementsByTag("td");
            	String tupel = "";
            	for(Element td:tds){
            		tupel +="\t"+td.text();
            	}
            	fw.append(tupel.replaceFirst("\t", "")+"\n");
            }
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//parse next page
		Element link = doc.getElementsByClass("next_page").select("a").first();

		if(!(link==null)){
        	//System.out.println(link);
        	nextPageLink = link.attr("href");
        	parseFollowingPages(basicLink, nextPageLink, conn, fw);
    	}else{
    		//reached last page
    		String lastPageNumber = nextPageLink.split("page=")[1].split("&")[0];
    		System.out.println("Next page = "+lastPageNumber);
    	}
	}
}
