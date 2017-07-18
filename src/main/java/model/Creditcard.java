package model;

import java.util.Date;

public class Creditcard {
	
	private int creditcard_id;
	private String cardname;
	private String cardnumber;
	private Date expiry_date;
	
	
	public Creditcard(int creditcard_id, String cardname, String cardnumber, Date expiry_date) {
		super();
		this.creditcard_id = creditcard_id;
		this.cardname = cardname;
		this.cardnumber = cardnumber;
		this.expiry_date = expiry_date;
	}
	
	public int getCreditcard_id() {
		return creditcard_id;
	}
	public void setCreditcard_id(int creditcard_id) {
		this.creditcard_id = creditcard_id;
	}
	public String getCardname() {
		return cardname;
	}
	public void setCardname(String cardname) {
		this.cardname = cardname;
	}
	public String getCardnumber() {
		return cardnumber;
	}
	public void setCardnumber(String cardnumber) {
		this.cardnumber = cardnumber;
	}
	public Date getExpiry_date() {
		return expiry_date;
	}
	public void setExpiry_date(Date expiry_date) {
		this.expiry_date = expiry_date;
	}

	@Override
	public String toString() {
		return "Creditcard [creditcard_id=" + creditcard_id + ", cardname=" + cardname + ", cardnumber=" + cardnumber
				+ ", expiry_date=" + expiry_date + "]";
	}
	
	
	
	

}
