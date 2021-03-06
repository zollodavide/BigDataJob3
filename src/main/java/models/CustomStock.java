package models;

public class CustomStock {

	private String ticker;
	private Double close;
	private String azienda;
	private Integer anno;
	private Integer mese;
	private Integer giorno;

	
	public CustomStock() {
		
	}

	

	public String getAzienda() {
		return azienda;
	}



	public void setAzienda(String azienda) {
		this.azienda = azienda;
	}



	public String getTicker() {
		return ticker;
	}


	public void setTicker(String ticker) {
		this.ticker = ticker;
	}


	public Double getClose() {
		return close;
	}


	public void setClose(Double close) {
		this.close = close;
	}


	public Integer getAnno() {
		return anno;
	}


	public void setAnno(Integer anno) {
		this.anno = anno;
	}


	public Integer getMese() {
		return mese;
	}


	public void setMese(Integer mese) {
		this.mese = mese;
	}


	public Integer getGiorno() {
		return giorno;
	}


	public void setGiorno(Integer giorno) {
		this.giorno = giorno;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((anno == null) ? 0 : anno.hashCode());
		result = prime * result + ((close == null) ? 0 : close.hashCode());
		result = prime * result + ((giorno == null) ? 0 : giorno.hashCode());
		result = prime * result + ((mese == null) ? 0 : mese.hashCode());
		result = prime * result + ((ticker == null) ? 0 : ticker.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomStock other = (CustomStock) obj;
		if (anno == null) {
			if (other.anno != null)
				return false;
		} else if (!anno.equals(other.anno))
			return false;
		if (close == null) {
			if (other.close != null)
				return false;
		} else if (!close.equals(other.close))
			return false;
		if (giorno == null) {
			if (other.giorno != null)
				return false;
		} else if (!giorno.equals(other.giorno))
			return false;
		if (mese == null) {
			if (other.mese != null)
				return false;
		} else if (!mese.equals(other.mese))
			return false;
		if (ticker == null) {
			if (other.ticker != null)
				return false;
		} else if (!ticker.equals(other.ticker))
			return false;
		return true;
	}



	@Override
	public String toString() {
		return "CustomStock [ticker=" + ticker + ", close=" + close + ", azienda=" + azienda + ", anno=" + anno
				+ ", mese=" + mese + ", giorno=" + giorno + "]";
	}



	
	
	
}
