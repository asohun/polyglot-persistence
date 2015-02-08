package com.polyglot.avro;

import org.apache.avro.reflect.AvroSchema;

@AvroSchema("{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"ch.chbtechnologies.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favouriteNumber\",\"type\":[\"int\",\"null\"]},{\"name\":\"favouriteColour\",\"type\":[\"null\",\"string\"]}]}")
public class User {

	private String name;
	private Integer favouriteNumber;
	private String favouriteColour;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getFavouriteNumber() {
		return favouriteNumber;
	}

	public void setFavouriteNumber(Integer favouriteNumber) {
		this.favouriteNumber = favouriteNumber;
	}

	public String getFavouriteColour() {
		return favouriteColour;
	}

	public void setFavouriteColour(String favouriteColour) {
		this.favouriteColour = favouriteColour;
	}

}
