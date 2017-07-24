package model;

import java.util.HashSet;

public class Product {
	
	private int product_id;
	private String product_name;
	private String product_description;
	private float price;
	private float in_stock;
	private HashSet<String> categories;
	
	public Product(int product_id, String product_name, String product_description, float price, float in_stock) {
		super();
		this.product_id = product_id;
		this.product_name = product_name;
		this.product_description = product_description;
		this.price = price;
		this.in_stock = in_stock;
	}
	
	public int getProduct_id() {
		return product_id;
	}
	public void setProduct_id(int product_id) {
		this.product_id = product_id;
	}
	public String getProduct_Name() {
		return product_name;
	}
	public void setProduct_Name(String product_name) {
		this.product_name = product_name;
	}
	public String getProduct_Description() {
		return product_description;
	}
	public void setDescription(String product_description) {
		this.product_description = product_description;
	}
	public float getPrice() {
		return price;
	}
	public void setPrice(float price) {
		this.price = price;
	}
	public float getIn_stock() {
		return in_stock;
	}
	public void setIn_stock(float in_stock) {
		this.in_stock = in_stock;
	}
	
	public HashSet<String> getCategories() {
		return categories;
	}

	public void setCategories(HashSet<String> categories) {
		this.categories = categories;
	}

}
