package model;

import java.util.Date;

public class Person {
	
	private int person_id;
	private String firstname;
	private String lastname;
	private Date birthdate;
	private String phone;
	private String country;
	private String city;
	private String zip;
	private String street;
	private Creditcard creditcard;
	
	public Person(int person_id, String firstname, String lastname, Date birthdate, String phone, String country,
			String city, String zip, String street, Creditcard creditcard) {
		super();
		this.person_id = person_id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.birthdate = birthdate;
		this.phone = phone;
		this.country = country;
		this.city = city;
		this.zip = zip;
		this.street = street;
		this.creditcard = creditcard;
	}

	public int getPerson_id() {
		return person_id;
	}

	public void setPerson_id(int person_id) {
		this.person_id = person_id;
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public Date getBirthdate() {
		return birthdate;
	}

	public void setBirthdate(Date birthdate) {
		this.birthdate = birthdate;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public Creditcard getCreditcard() {
		return creditcard;
	}

	public void setCreditcard_id(Creditcard creditcard) {
		this.creditcard = creditcard;
	}
	
	
	

}
