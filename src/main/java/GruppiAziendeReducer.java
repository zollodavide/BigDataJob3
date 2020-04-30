import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


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
					
				}catch(Exception e) {
					
				}
			}
			else if(parts.length == 1) 
				ticker2azienda.put(key.toString(), parts[0]);
			
		}
		
	}
	
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		
		

	}	

}
