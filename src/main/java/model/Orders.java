package model;

import java.util.Date;

public class Orders {
	
	private int order_id;
	private Date order_date;
	private Shopuser user;
	
	
	public Orders(int order_id, Date order_date, Shopuser user) {
		super();
		this.order_id = order_id;
		this.order_date = order_date;
		this.user = user;
	}


	public int getOrder_id() {
		return order_id;
	}


	public void setOrder_id(int order_id) {
		this.order_id = order_id;
	}


	public Date getOrder_date() {
		return order_date;
	}


	public void setOrder_date(Date order_date) {
		this.order_date = order_date;
	}


	public Shopuser getUser() {
		return user;
	}


	public void setUsername(Shopuser use) {
		this.user = user;
	}


	@Override
	public String toString() {
		return "Orders [order_id=" + order_id + ", order_date=" + order_date + ", user=" + user + "]";
	}
	
	
	

}
