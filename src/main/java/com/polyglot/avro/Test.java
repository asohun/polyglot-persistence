package com.polyglot.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class Test {

	private static File file;

	private static Schema schema;

	private static AvroUser avroUser;

	private static User user;

	public static void main(String[] args) throws IOException {
		file = new File("E:\\Anoop\\User.avro");
		schema = new Schema.Parser().parse(AvroUser.class
				.getResourceAsStream("/avro/AvroUser.avsc"));

		avroUser = new AvroUser();
		avroUser.setName("Avro User");
		// avroUser.setFavouriteColour("Favourite Colour");
		avroUser.setFavouriteNumber(666);

		user = new User();
		user.setName("Avro User");
		// user.setFavouriteColour("Favourite Colour");
		user.setFavouriteNumber(666);

		// serializeUsingCodeGeneration();
		// deserializeUsingCodeGeneration();

		// serializeGeneric();
		// deserializeGeneric();

		serializeUsingReflection();
		deserializeUsingReflection();
	}

	private static void serializeUsingReflection() throws IOException {
		DatumWriter<User> userDatumWriter = new ReflectDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(user);
		dataFileWriter.close();
	}

	private static void deserializeUsingReflection() throws IOException {
		DatumReader<User> userDatumReader = null;

		// If class is used instead of schema, the schema should be present in
		// the class through the @AvroSchema annotation
		userDatumReader = new ReflectDatumReader<User>(User.class);
		// userDatumReader = new ReflectDatumReader<User>(schema);

		DataFileReader<User> dataFileReader = new DataFileReader<User>(file,
				userDatumReader);

		User user = null;
		while (dataFileReader.hasNext()) {
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}

	private static void serializeGeneric() throws IOException {
		GenericRecord genericUser = new GenericData.Record(schema);
		genericUser.put("name", user.getName());
		genericUser.put("favouriteNumber", user.getFavouriteNumber());
		// Leave favorite color null

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
				schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
				datumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(genericUser);
		dataFileWriter.close();
	}

	private static void deserializeGeneric() throws IOException {
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);
		GenericRecord user = null;
		while (dataFileReader.hasNext()) {
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}

	private static void serializeUsingCodeGeneration() throws IOException {
		DatumWriter<AvroUser> userDatumWriter = new SpecificDatumWriter<AvroUser>(
				AvroUser.class);
		DataFileWriter<AvroUser> dataFileWriter = new DataFileWriter<AvroUser>(
				userDatumWriter);
		dataFileWriter.create(schema, file);
		dataFileWriter.append(avroUser);
		dataFileWriter.close();
	}

	private static void deserializeUsingCodeGeneration() throws IOException {
		DatumReader<AvroUser> userDatumReader = new SpecificDatumReader<AvroUser>(
				AvroUser.class);
		DataFileReader<AvroUser> dataFileReader = new DataFileReader<AvroUser>(
				file, userDatumReader);

		AvroUser user = null;
		while (dataFileReader.hasNext()) {
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}

}
