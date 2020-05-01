package mapred;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.text.Utilities;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import constants.ReducerConstants;
import models.CustomStock;
import utility.GruppiUtility;


public class GruppiAziendeReducer extends Reducer<Text, Text, Text, Text> {
		
	private Map<String, String> ticker2azienda = new HashMap<String, String>();
	private Map<String, List<CustomStock>> ticker2stocks = new HashMap<String, List<CustomStock>>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		List<CustomStock> stocks;
		
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
					
					if(ticker2stocks.containsKey(key.toString()))
						stocks = ticker2stocks.get(key.toString());
					else
						stocks = new ArrayList<CustomStock>();
					
					stocks.add(custom);
					ticker2stocks.put(key.toString(), stocks);
					
				}catch(Exception e) {
					
				}
			}
			else if(parts.length == 1) 
				ticker2azienda.put(key.toString(), parts[0]);
			
		}
		
	}
	
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		Map<String, List<CustomStock>> azienda2stock2016 = new HashMap<String, List<CustomStock>>();
		Map<String, List<CustomStock>> azienda2stock2017 = new HashMap<String, List<CustomStock>>();
		Map<String, List<CustomStock>> azienda2stock2018 = new HashMap<String, List<CustomStock>>();
		
		for(String tick : ticker2stocks.keySet()) {
			
			String azienda = ticker2azienda.get(tick);
			List<CustomStock> val = ticker2stocks.get(tick);
			
			List<CustomStock> lista2016 = new ArrayList<CustomStock>(); 
			List<CustomStock> lista2017 = new ArrayList<CustomStock>(); 
			List<CustomStock> lista2018 = new ArrayList<CustomStock>(); 
			
			for(CustomStock stock : val) {	
				if(stock.getAnno()==2016) 
					lista2016.add(stock);
				else if(stock.getAnno()==2017)	
					lista2017.add(stock);
				else
					lista2018.add(stock);				
			}

			if (lista2016.size()!=0)
				azienda2stock2016.put(azienda, lista2016); //OK
			
			if (lista2017.size()!=0)
				azienda2stock2017.put(azienda, lista2017);
			
			
			if (lista2018.size()!=0)
				azienda2stock2018.put(azienda, lista2018);		
		}
		
		
		
		Map<Integer, List<String>> trend2azienda2016  = new HashMap<Integer, List<String>>();
		Map<Integer, List<String>> trend2azienda2017  = new HashMap<Integer, List<String>>();
		Map<Integer, List<String>> trend2azienda2018  = new HashMap<Integer, List<String>>();
		
		
		for(String azienda : azienda2stock2016.keySet()) {
			
			List<CustomStock> lst = azienda2stock2016.get(azienda);

			CustomStock min = null;
			CustomStock max = null;
			
			for(CustomStock stock : lst) {
				if(min == null || GruppiUtility.dataMinore(stock, min)) 
					min = stock;
				if(max == null || GruppiUtility.dataMaggiore(stock, max))
					max =stock;
			}
			
			Integer trend = (int) Math.round((max.getClose() - min.getClose())/max.getClose());	
			List<String> aziende;
			if(trend2azienda2016.containsKey(trend)) 
				aziende = trend2azienda2016.get(trend);
			else
				aziende  = new ArrayList<String>();
			
			aziende.add(azienda);
			trend2azienda2016.put(trend, aziende);
		}
		

		for(String azienda : azienda2stock2017.keySet()) {
			
			List<CustomStock> lst = azienda2stock2017.get(azienda);

			CustomStock min = null;
			CustomStock max = null;
			
			for(CustomStock stock : lst) {
				if(min == null || GruppiUtility.dataMinore(stock, min)) 
					min = stock;
				if(max == null || GruppiUtility.dataMaggiore(stock, max))
					max =stock;
			}
			
			Integer trend = (int) Math.round((max.getClose() - min.getClose())/max.getClose());	
			List<String> aziende;
			if(trend2azienda2017.containsKey(trend)) 
				aziende = trend2azienda2017.get(trend);
			else
				aziende  = new ArrayList<String>();
			
			aziende.add(azienda);
			trend2azienda2017.put(trend, aziende);
		}
		
		
		for(String azienda : azienda2stock2018.keySet()) {
			
			List<CustomStock> lst = azienda2stock2018.get(azienda);

			CustomStock min = null;
			CustomStock max = null;
			
			for(CustomStock stock : lst) {
				if(min == null || GruppiUtility.dataMinore(stock, min)) 
					min = stock;
				if(max == null || GruppiUtility.dataMaggiore(stock, max))
					max =stock;
			}
			
			Integer trend = (int) Math.round((max.getClose() - min.getClose())/max.getClose());	
			List<String> aziende;
			if(trend2azienda2018.containsKey(trend)) 
				aziende = trend2azienda2018.get(trend);
			else
				aziende  = new ArrayList<String>();
			
			aziende.add(azienda);
			trend2azienda2018.put(trend, aziende);
		}
		
		
		for(Integer i : trend2azienda2016.keySet()) 
			context.write(new Text("2016 "+ i.toString() + "%"), new Text(trend2azienda2016.get(i).toString()));

		for(Integer i : trend2azienda2017.keySet()) 
			context.write(new Text("2017 "+ i.toString() + "%"), new Text(trend2azienda2017.get(i).toString()));

		for(Integer i : trend2azienda2018.keySet()) 
			context.write(new Text("2018 "+ i.toString() + "%"), new Text(trend2azienda2018.get(i).toString()));

		
		
		

	}	

}
