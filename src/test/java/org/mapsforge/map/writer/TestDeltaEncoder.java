package org.mapsforge.map.writer;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

/**
 * @author bross
 */
public class TestDeltaEncoder {

	private List<Integer> mockCoordinates;

	/**
	 */
	@Before
	public void setUp() {
		this.mockCoordinates = new ArrayList<Integer>();

		this.mockCoordinates.add(Integer.valueOf(52000000));
		this.mockCoordinates.add(Integer.valueOf(13000000));

		this.mockCoordinates.add(Integer.valueOf(52000100));
		this.mockCoordinates.add(Integer.valueOf(13000100));

		this.mockCoordinates.add(Integer.valueOf(52000500));
		this.mockCoordinates.add(Integer.valueOf(13000500));

		this.mockCoordinates.add(Integer.valueOf(52000400));
		this.mockCoordinates.add(Integer.valueOf(13000400));

		this.mockCoordinates.add(Integer.valueOf(52000800));
		this.mockCoordinates.add(Integer.valueOf(13000800));

		this.mockCoordinates.add(Integer.valueOf(52001000));
		this.mockCoordinates.add(Integer.valueOf(13001000));
	}

	/**
	 * 
	 */
	@Test
	public void testDeltaEncode() {
		List<Integer> deltaEncoded = DeltaEncoder.deltaEncode(this.mockCoordinates);
		Assert.assertEquals(Integer.valueOf(52000000), deltaEncoded.get(0));
		Assert.assertEquals(Integer.valueOf(13000000), deltaEncoded.get(1));
		Assert.assertEquals(Integer.valueOf(100), deltaEncoded.get(2));
		Assert.assertEquals(Integer.valueOf(100), deltaEncoded.get(3));
		Assert.assertEquals(Integer.valueOf(400), deltaEncoded.get(4));
		Assert.assertEquals(Integer.valueOf(400), deltaEncoded.get(5));
		Assert.assertEquals(Integer.valueOf(-100), deltaEncoded.get(6));
		Assert.assertEquals(Integer.valueOf(-100), deltaEncoded.get(7));
		Assert.assertEquals(Integer.valueOf(400), deltaEncoded.get(8));
		Assert.assertEquals(Integer.valueOf(400), deltaEncoded.get(9));
		Assert.assertEquals(Integer.valueOf(200), deltaEncoded.get(10));
		Assert.assertEquals(Integer.valueOf(200), deltaEncoded.get(11));
	}

	/**
	 * 
	 */
	@Test
	public void testDoubleDeltaEncode() {
		List<Integer> ddeltaEncoded = DeltaEncoder.doubleDeltaEncode(this.mockCoordinates);
		Assert.assertEquals(Integer.valueOf(52000000), ddeltaEncoded.get(0));
		Assert.assertEquals(Integer.valueOf(13000000), ddeltaEncoded.get(1));
		Assert.assertEquals(Integer.valueOf(100), ddeltaEncoded.get(2));
		Assert.assertEquals(Integer.valueOf(100), ddeltaEncoded.get(3));
		Assert.assertEquals(Integer.valueOf(300), ddeltaEncoded.get(4));
		Assert.assertEquals(Integer.valueOf(300), ddeltaEncoded.get(5));
		Assert.assertEquals(Integer.valueOf(-500), ddeltaEncoded.get(6));
		Assert.assertEquals(Integer.valueOf(-500), ddeltaEncoded.get(7));
		Assert.assertEquals(Integer.valueOf(500), ddeltaEncoded.get(8));
		Assert.assertEquals(Integer.valueOf(500), ddeltaEncoded.get(9));
		Assert.assertEquals(Integer.valueOf(-200), ddeltaEncoded.get(10));
		Assert.assertEquals(Integer.valueOf(-200), ddeltaEncoded.get(11));
	}

}
