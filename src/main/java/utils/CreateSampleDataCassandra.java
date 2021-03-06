package utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import model.Creditcard;
import model.Orderline;
import model.Orders;
import model.Person;
import model.Product;
import model.Shopuser;

public class CreateSampleDataCassandra {
	
	private ArrayList<Creditcard> creditcardlist = new ArrayList<Creditcard>();
	private ArrayList<Person> personlist = new ArrayList<Person>();
	private ArrayList<Product> productlist = new ArrayList<Product>();
	private ArrayList<Shopuser> shopuserlist = new ArrayList<Shopuser>();
	private ArrayList<Orders> orderlist = new ArrayList<Orders>();
	private HashSet<Orderline> orderlinelist = new HashSet<Orderline>();

	
	public static void main(String[] argv) {
		CreateSampleDataCassandra csdc = new CreateSampleDataCassandra();
		
		csdc.fillCreditCards();
		csdc.fillPersons();
		csdc.fillProducts();
		csdc.fillShopusers();
		csdc.fillOrders();
		//csdc.fillOrderline();
		csdc.fillOrderline2();
		
		System.out.println("size="+csdc.orderlinelist.size());
		
		//Write to DB:
		
		Session session = ConnectionHelperCassandra.getDBConnection();
		csdc.writeOrdersToCassandra(session);
		//csdc.writeOrderlinesToCassandra(session);
		
	     System.out.println("Done");
	     session.close();
	     session.getCluster().close();
	     System.exit(0);
	
	}
	
	// ---------------------------------------------------------------
	
	
	/**
	 * Fill the ArrayList with 10000 sample Creditcard Objects
	 */
	private void fillCreditCards() {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
		Creditcard c1;
		for (int i = 1; i <= 10000; i++) {
			String cardname = "Visa";
			if(i%3==0) {
				cardname = "Mastercard";
			}
			else if (i%8 == 0) {
				cardname = "Amex";
			}
			
			c1 = new Creditcard(i, cardname, "1234"+i, DateHelper.getCurrentTimeStampAsDate());
			creditcardlist.add(c1);
		}
		endtime = System.nanoTime();
		System.out.println("Duration of fillCreditCards (ms): "+(endtime-starttime)/1000000);
		
	}
	
	

	/**
	 * Fill the ArrayList with 10000 sample Person Objects
	 */
	private void fillPersons() {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
		Person p1;
		for (int i = 1; i <= 10000; i++) {
			String country = "Austria";
			if(i%4==0) {
				country = "Germany";
			}
			else if (i%13 == 0) {
				country = "Italy";
			} 
			else if (i%7 == 0) {
				country = "France";
			}
			p1 = new Person(i, "Firstname"+i, "Lastname"+i, new java.util.Date(), "0664"+i, country, "Testcity", (i+1000)+"", "Teststreet "+i, creditcardlist.get(i-1));
			personlist.add(p1);
		}
		endtime = System.nanoTime();
		System.out.println("Duration of fillPersons (ms): "+(endtime-starttime)/1000000);
		
	}

	/**
	 * Fill the ArrayList with 100000 sample Product Objects
	 */
	private void fillProducts() {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
		Random randy = new Random();
		
		Product p1;
		for (int i = 1; i <= 1000000; i++) {
			p1 = new Product(i, "Product"+i, "The best Product!", randy.nextFloat()*randy.nextInt(1000), randy.nextFloat()*randy.nextInt(500));
			fillProducts2Categories(p1);
			productlist.add(p1);
		}
		endtime = System.nanoTime();
		System.out.println("Duration of fillProducts (ms): "+(endtime-starttime)/1000000);
		
	}
	
	/**
	 * Fill some sample Categories into the Products
	 */
	private void fillProducts2Categories(Product p) {
		Random randy = new Random();
		HashSet<String> cats = new HashSet<String>();
		//set up to 10 categories for the product
		for(int j = 1; j <= randy.nextInt(9)+1; j++) {
			cats.add("category"+j);
		}
		p.setCategories(cats);	
}
	
	/**
	 * Fill the ArrayList with 10000 sample Shopuser Objects
	 */
	private void fillShopusers() {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		

		Shopuser s1;
		for (int i = 1; i <= 10000; i++) {
			
			java.util.Date d = null;
			if(i%27 == 0) {
				d = new java.util.Date();
			}
			
			//create MD5 Hash of Password
			String pwd = "pwd"+i;
			byte[] bytesOfMessage = null;
			try {
				bytesOfMessage = pwd.getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			MessageDigest md = null;
			try {
				md = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			byte[] thedigest = md.digest(bytesOfMessage);
			StringBuffer sb = new StringBuffer();
			for (byte b : thedigest) {
				sb.append(String.format("%02x", b & 0xff));
			}
			pwd = sb.toString();

			s1 = new Shopuser("user"+i, "user"+i+"@testmail.at", pwd, new java.util.Date(), d, personlist.get(i-1));
			shopuserlist.add(s1);
		}
		endtime = System.nanoTime();
		System.out.println("Duration of fillShopusers (ms): "+(endtime-starttime)/1000000);
		
	}
	
	
	/**
	 * Fill the ArrayList with 100000 sample Order Objects
	 */
	private void fillOrders() {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
		Random randy = new Random();

		Shopuser user;
		Orders o1;
		for(int i = 1; i <= 100000; i++) {
			
			// generate a random date
			Calendar calendar = Calendar.getInstance();
			calendar.set(Calendar.YEAR, randy.nextInt(17)+2000);
			calendar.set(Calendar.DAY_OF_YEAR, randy.nextInt(365));
			java.util.Date d = calendar.getTime();
			
			// get a random user from the user list
			user = shopuserlist.get(randy.nextInt(10000));
			
			o1 = new Orders(i, d, user);
			orderlist.add(o1);
		}
		
		
		endtime = System.nanoTime();
		System.out.println("Duration of fillOrders (ms): "+(endtime-starttime)/1000000);
		
	}
	

	/**
	 * Fill the ArrayList with 10000000 sample Orderline Objects
	 */
	private void fillOrderline() {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
		Random randy = new Random();

		Product p;
		Orders o;
		
		Orderline o1;
		
		for(int i = 1; i <= 10000000; i++) {
			o = orderlist.get(randy.nextInt(100000));
			p = productlist.get(randy.nextInt(1000000));
			
			
			o1 = new Orderline(i, o, p);
			o1.setAmount(randy.nextFloat()+randy.nextInt(1000));
			orderlinelist.add(o1);
		}
		
		//for (Orderline ol : orderlinelist) {
		//	ol.setAmount(randy.nextFloat()+randy.nextInt(1000));
		//}
		
		endtime = System.nanoTime();
		System.out.println("Duration of fillOrderline (ms): "+(endtime-starttime)/1000000);
		
	}
	

	/**
	 * Alternative: Fill the ArrayList with 5000000 sample Orderline Objects
	 */
	private void fillOrderline2() {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
		Random randy = new Random();

		Product p;
		Orders o;
		
		Orderline o1;
		int i = 1;
				
		while(i <= 5000000/*orderlinelist.size() < 5000*/) {
			o = orderlist.get(randy.nextInt(100000));
			p = productlist.get(randy.nextInt(1000000));
			
			
			o1 = new Orderline(i, o, p);
			o1.setAmount(randy.nextFloat()+randy.nextInt(1000));
			orderlinelist.add(o1);
			i++;
		}
		
		endtime = System.nanoTime();
		System.out.println("Duration of fillOrderline2 (ms): "+(endtime-starttime)/1000000);
		
	}
	
	
	// ---------------------------------------------------------------------------
	
	
	/**
	 * Write Orders into Cassandra DB
	 */
	private void writeOrdersToCassandra(Session session) {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
        PreparedStatement prep = session.prepare("INSERT INTO orders (username, orderid, orderdate, productid, amount, prod_cat) VALUES (?, ?, ?, ?, ?, ?) "
        		//+ "IF NOT EXISTS" 
        		+ "");
        BoundStatement bound = null;
        
		String username = null;
		int orderid = 0;
		int productid = 0;
		float amount = 0;
		Date orderdate = null;
        
		for (Orderline o1 : orderlinelist) {
			username = o1.getOrder().getUser().getUsername();
			orderid = o1.getOrder().getOrder_id();
			orderdate = o1.getOrder().getOrder_date();
			productid = o1.getProduct().getProduct_id();
			amount = o1.getAmount();
						
			bound = new BoundStatement(prep);
	        bound.bind(username, orderid, 
	        		com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(orderdate.getTime()) , 
	        		productid, amount,
	        		o1.getProduct().getCategories());
	        session.executeAsync(bound);
		}
		endtime = System.nanoTime();
		System.out.println("Duration of writeOrdersToCassandra (ms): "+(endtime-starttime)/1000000);
		
	}
	
	

	/**
	 * Write Orders into Cassandra DB
	 */
	private void writeOrderlinesToCassandra(Session session) {
		long starttime;
		long endtime;
		starttime = System.nanoTime();
		
        PreparedStatement prep = session.prepare("INSERT INTO orderline (orderlineid, username, orderid, orderdate, productid, amount) "
        		+ "VALUES (?, ?, ?, ?, ?, ?) ");

		for (Orderline o1 : orderlinelist) {
			String username = o1.getOrder().getUser().getUsername();
			int orderid = o1.getOrder().getOrder_id();
			int productid = o1.getProduct().getProduct_id();
			float amount = o1.getAmount();
			Date orderdate = o1.getOrder().getOrder_date();
			int orderlineid = o1.getOrderline_id();
						
			BoundStatement bound = new BoundStatement(prep);
	        bound.bind(orderlineid, username, orderid, com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(orderdate.getTime()) , productid, amount);
	        session.executeAsync(bound);
			
		}
		endtime = System.nanoTime();
		System.out.println("Duration of writeOrderlinesToCassandra (ms): "+(endtime-starttime)/1000000);
		
	}
	
	
	
	
	
}
