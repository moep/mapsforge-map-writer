/*
 * Copyright 2010, 2011 mapsforge.org
 *
 * This program is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.mapsforge.storage.tile;

/**
 * This container stores a single tile's raw data and its type.
 * 
 * @author Karsten Groll
 * 
 */
public class TileDataContainer {
	/** This tile has an unknown type */
	public static final byte TILE_TYPE_INVALID = -1;
	/** This tile contains vector data */
	public static final byte TILE_TYPE_VECTOR = 1;
	/** This tile is a PNG graphic */
	public static final byte TILE_TYPE_PNG = 2;

	private byte[] data;
	private byte tileType;
	private int xPos;
	private int yPos;
	private int baseZoomLevel;

	/**
	 * 
	 * @param data
	 *            The tile's data.
	 * @param tileType
	 *            What kind of data does this tile contain? (Vector, PNG)
	 * @param xPos
	 *            The tile's x-coordinate in the grid.
	 * @param yPos
	 *            The tile's y-coordinate in the grid.
	 * @param baseZoomLevel
	 *            The tile's base zoom level.
	 */
	public TileDataContainer(byte[] data, byte tileType, int xPos, int yPos, byte baseZoomLevel) {
		setData(data);
		setTileType(tileType);
		setXPos(xPos);
		setYPos(yPos);
		setBaseZoomInterval(baseZoomLevel);
	}

	/**
	 * 
	 * @return The tile's data.
	 */
	public byte[] getData() {
		return this.data;
	}

	/**
	 * 
	 * @param data
	 *            The tile's data.
	 */
	public void setData(byte[] data) {
		this.data = data;
	}

	/**
	 * 
	 * @return The tile's type.
	 */
	public byte getTileType() {
		return this.tileType;
	}

	/**
	 * 
	 * @param tileType
	 *            What kind of data does this tile contain? (Vector, PNG)
	 */
	public void setTileType(byte tileType) {
		// Check for valid tile type
		if (tileType != TILE_TYPE_VECTOR && tileType != TILE_TYPE_PNG) {
			this.tileType = TILE_TYPE_INVALID;
		}

		this.tileType = tileType;
	}

	/**
	 * 
	 * @return The tile's x-coordinate in the grid.
	 */
	public int getxPos() {
		return xPos;
	}

	/**
	 * 
	 * @param xPos
	 *            The tile's x-coordinate in the grid.
	 */
	public void setXPos(int xPos) {
		this.xPos = xPos;
	}

	/**
	 * 
	 * @return The tile's y-coordinate in the grid.
	 */
	public int getyPos() {
		return yPos;
	}

	/**
	 * 
	 * @param yPos
	 *            The tile's y-coordinate in the grid.
	 */
	public void setYPos(int yPos) {
		this.yPos = yPos;
	}

	/**
	 * 
	 * @return The tile's base zoom level.
	 */
	public int getBaseZoomLevel() {
		return baseZoomLevel;
	}

	/**
	 * 
	 * @param baseZoomInterval
	 *            The tile's base zoom interval.
	 */
	public void setBaseZoomInterval(int baseZoomInterval) {
		this.baseZoomLevel = baseZoomInterval;
	}
}
