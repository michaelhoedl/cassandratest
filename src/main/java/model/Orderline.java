package model;

public class Orderline {
	
	private int orderline_id;
	private Orders order;
	private Product product;
	private float amount;
	
	public Orderline(int orderline_id, Orders order, Product product, float amount) {
		super();
		this.orderline_id = orderline_id;
		this.order = order;
		this.product = product;
		this.amount = amount;
	}
	
	

	public Orderline(int orderline_id, Orders order, Product product) {
		super();
		this.orderline_id = orderline_id;
		this.order = order;
		this.product = product;
	}


	

	public int getOrderline_id() {
		return orderline_id;
	}



	public void setOrderline_id(int orderline_id) {
		this.orderline_id = orderline_id;
	}



	public Orders getOrder() {
		return order;
	}

	public void setOrder(Orders order) {
		this.order = order;
	}

	public Product getProduct() {
		return product;
	}

	public void setProduct(Product product) {
		this.product = product;
	}

	public float getAmount() {
		return amount;
	}

	public void setAmount(float amount) {
		this.amount = amount;
	}

	@Override
	public String toString() {
		return "Orderline [order=" + order + ", product=" + product + ", amount=" + amount + "]";
	}
	
	
	

}
