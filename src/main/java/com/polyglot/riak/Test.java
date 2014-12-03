package com.polyglot.riak;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

public class Test {

	public static void main(String[] args) {
		try {
			get();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void test() throws UnknownHostException, ExecutionException,
			InterruptedException {
		RiakClient client = RiakClient.newClient("192.168.1.168");
		Namespace ns = new Namespace("default", "my_bucket");
		Location location = new Location(ns, "my_key");
		RiakObject riakObject = new RiakObject();
		riakObject.setValue(BinaryValue.create("my_value"));
		StoreValue store = new StoreValue.Builder(riakObject)
				.withLocation(location).withOption(Option.W, new Quorum(3))
				.build();
		client.execute(store);
	}

	public static void get() throws UnknownHostException, ExecutionException,
			InterruptedException {
		RiakClient client = RiakClient.newClient("192.168.1.167");
		Namespace ns = new Namespace("default", "my_bucket");
		Location location = new Location(ns, "my_key");
		FetchValue fv = new FetchValue.Builder(location).build();
		FetchValue.Response response = client.execute(fv);
		RiakObject obj = response.getValue(RiakObject.class);
		System.out.println(obj.getValue().toString());
	}
}
