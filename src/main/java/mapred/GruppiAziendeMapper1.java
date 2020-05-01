package mapred;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import constants.HistoricalStockPricesConstants;


public class GruppiAziendeMapper1  extends Mapper<Object, Text, Text, Text>{
	
	
	private Text ticker = new Text();
	private Text closingPrice = new Text();
	private Integer anno;
	private Integer mese;
	private Integer giorno;

	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] parts = value.toString().split(",");
		ticker.set(parts[HistoricalStockPricesConstants.TICKER]);
		
		String[] data = parts[HistoricalStockPricesConstants.DATE].split("-");

		try {
			
			closingPrice.set(parts[HistoricalStockPricesConstants.CLOSE]);		

			if(Integer.parseInt(data[0])>=2016 && Integer.parseInt(data[0])<=2018) {
				
				anno = Integer.parseInt(data[0]);
				mese = Integer.parseInt(data[1]);
				giorno = Integer.parseInt(data[2]);
				String all = closingPrice+","+anno+","+mese+","+giorno;
				context.write(ticker, new Text(all));
			}
		} catch(Exception e) {
			
		}		
	}

}
