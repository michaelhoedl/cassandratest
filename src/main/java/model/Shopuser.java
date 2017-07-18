package model;

import java.util.Date;

public class Shopuser {
	
	private String username;
	private String email;
	private String pwd;
	private Date created_date;
	private Date locked_date;
	private Person person;
	
	public Shopuser(String username, String email, String pwd, Date created_date, Date locked_date, Person person) {
		super();
		this.username = username;
		this.email = email;
		this.pwd = pwd;
		this.created_date = created_date;
		this.locked_date = locked_date;
		this.person = person;
	}
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getPwd() {
		return pwd;
	}
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
	public Date getCreated_date() {
		return created_date;
	}
	public void setCreated_date(Date created_date) {
		this.created_date = created_date;
	}
	public Date getLocked_date() {
		return locked_date;
	}
	public void setLocked_date(Date locked_date) {
		this.locked_date = locked_date;
	}
	public Person getPerson() {
		return person;
	}
	public void setPerson(Person person) {
		this.person = person;
	}

	@Override
	public String toString() {
		return "Shopuser [username=" + username + ", email=" + email + ", pwd=" + pwd + ", created_date=" + created_date
				+ ", locked_date=" + locked_date + ", person=" + person + "]";
	}
	
	
	
	
	
	

}
