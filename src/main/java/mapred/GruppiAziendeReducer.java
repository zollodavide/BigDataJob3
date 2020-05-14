package mapred;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.swing.text.Utilities;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import constants.ReducerConstants;
import models.CustomStock;
import models.Tuple;
import utility.GruppiUtility;


public class GruppiAziendeReducer extends Reducer<Text, Text, Text, Text> {
		
	private Map<String, String> ticker2azienda = new HashMap<String, String>();
	private List<CustomStock> stocks = new ArrayList<CustomStock>();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		for( Text value: values ) {
			
			String[] parts = value.toString().split(",");

			if(parts.length > 1) {

				CustomStock custom = new CustomStock();
				
				try {					
					custom.setTicker(key.toString());
					custom.setClose(Double.parseDouble(parts[ReducerConstants.CLOSING]));
					custom.setAnno(Integer.parseInt(parts[ReducerConstants.ANNO]));
					custom.setMese(Integer.parseInt(parts[ReducerConstants.MESE]));
					custom.setGiorno(Integer.parseInt(parts[ReducerConstants.GIORNO]));
					
					this.stocks.add(custom);
				}catch(Exception e) {
					
				}
			}
			else if(parts.length == 1) 
				ticker2azienda.put(key.toString(), parts[0]);
			
		}
		
	}
	
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		for(CustomStock st : this.stocks) 
			st.setAzienda(ticker2azienda.get(st.getTicker()));

		
		Map<Tuple<String, Integer>,List<CustomStock>> aziendaAnno2stocks = new HashMap<Tuple<String,Integer>, List<CustomStock>>();
		
		for(CustomStock c : this.stocks) {
			String azienda = c.getAzienda();
			Integer anno = c.getAnno();
			Tuple<String,Integer> key = new Tuple<String, Integer>(azienda, anno);
			
			List<CustomStock> lst = new ArrayList<CustomStock>();
			if(aziendaAnno2stocks.containsKey(key))
				lst = aziendaAnno2stocks.get(key);
			lst.add(c);
			aziendaAnno2stocks.put(key, lst);
		}
		
		
		Map<String, List<Tuple<Integer,Integer>>> azienda2annoDiff = new HashMap<String, List<Tuple<Integer,Integer>>>();
		for(Tuple<String,Integer> key :  aziendaAnno2stocks.keySet()) {

			CustomStock minStock = null;
			CustomStock maxStock = null;
			
			for(CustomStock c : aziendaAnno2stocks.get(key)) {

				if(minStock == null || GruppiUtility.dataMinore(c, minStock))
					minStock = c;
				if(maxStock == null || GruppiUtility.dataMaggiore(c, maxStock))
					maxStock = c;
				
			}
			
			Integer meanDiff = (int) (((maxStock.getClose() - minStock.getClose())/minStock.getClose())*100);
			Tuple<Integer,Integer> value = new Tuple<Integer, Integer>(key.getSecond(), meanDiff);
			
			List<Tuple<Integer,Integer>> lst = new ArrayList<Tuple<Integer,Integer>>();
			if(azienda2annoDiff.containsKey(key.getFirst()))
				lst = azienda2annoDiff.get(key.getFirst());
			lst.add(value);
			azienda2annoDiff.put(key.getFirst(), lst);
		}
		
		
		Map<String, List<String>> gruppi = new HashMap<String, List<String>>();
		for(String azienda: azienda2annoDiff.keySet()) {

			String D2016 = "";
			String D2017 = "";
			String D2018 = "";
			
			for(Tuple<Integer,Integer> tup : azienda2annoDiff.get(azienda)) {
				if(tup.getFirst() == 2016)
					D2016 = tup.getSecond().toString();
				else if(tup.getFirst() == 2017)
					D2017 = tup.getSecond().toString();
				else if(tup.getFirst() == 2018)
					D2018 = tup.getSecond().toString();
			}
			if(D2016.length()==0)
				D2016 = "0";
			if(D2017.length()==0)
				D2017 = "0";
			if(D2018.length()==0)
				D2018 = "0";
			
			String key = D2016 + "% " + D2017 + "% " + D2018 +"%";
			
			List<String> aziendeValues = new ArrayList<String>();
			if(gruppi.containsKey(key)) 
				aziendeValues = gruppi.get(key);
			aziendeValues.add(azienda);
			gruppi.put(key, aziendeValues);
		}
		
		for(String k : gruppi.keySet() ) 
			context.write(new Text(k + " = "), new Text(gruppi.get(k).toString()));
		

	}	

}
