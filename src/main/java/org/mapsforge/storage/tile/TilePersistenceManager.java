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

import java.util.Collection;

import org.mapsforge.storage.dataExtraction.MapFileMetaData;

/**
 * This interface abstracts from an underlying tile-based map file format by providing methods for
 * inserting, updating and deleting tiles.
 * 
 * @author Karsten Groll
 * 
 *         TODO Do we need exceptions here? (Tile not found, invalid coordinates, ...)
 * 
 *         TODO Distinguish between image and vector tiles
 * 
 *         TODO Cache data tiles (does it make sense?)
 */
public interface TilePersistenceManager {

	/**
	 * Replaces a tile in the database with the given data. If the tile does not exist it will be
	 * created.
	 * 
	 * @param rawData
	 *            The tile's data in binary representation.
	 * @param xPos
	 *            The tile's x coordinate in the grid for the given base zoom level.
	 * @param yPos
	 *            The tile's y coordinate in the grid for the given base zoom level.
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 */
	public void insertOrUpdateTile(final byte[] rawData, final int xPos, final int yPos, final byte baseZoomInterval);

	/**
	 * Replaces a tile in the database with the given data. If the tile does not exist it will be
	 * created.
	 * 
	 * @param rawData
	 *            The tile's data in binary representation.
	 * @param id
	 *            The tile's coordinate for the given base zoom level in a 1-dimensional representation:
	 *            id = (y * 4^baseZoomLevel) + x
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 */
	public void insertOrUpdateTile(final byte[] rawData, final int id, final byte baseZoomInterval);

	/**
	 * Replaces a set of tiles in the database with the given data. If a tile does not exist it will be
	 * created. Use this method instead of {@link #insertOrUpdateTile(byte[], int, byte)} whenever you
	 * want to add a batch of tiles.
	 * 
	 * @param rawData
	 *            The tiles' data and meta data.
	 */
	public void insertOrUpdateTiles(final Collection<TileDataContainer> rawData);

	/**
	 * Deletes a tile at the specified position. If there is no such tile the methods does nothing.
	 * 
	 * @param xPos
	 *            The tile's x coordinate in the grid for the given base zoom level.
	 * @param yPos
	 *            The tile's y coordinate in the grid for the given base zoom level.
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 */
	public void deleteTile(final int xPos, final int yPos, final byte baseZoomInterval);

	/**
	 * Deletes a tile at the specified position. If there is no such tile the methods does nothing.
	 * 
	 * @param id
	 *            The tile's coordinate for the given base zoom level in a 1-dimensional representation:
	 *            id = (y * 4^baseZoomLevel) + x
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 */
	public void deleteTile(final int id, final byte baseZoomInterval);

	/**
	 * Deletes a set of tiles at the specified position. If there is no such tile the methods does
	 * nothing.
	 * 
	 * @param ids
	 *            The tiles' coordinates for the given base zoom level in a 1-dimensional
	 *            representation: id = (y * 4^baseZoomLevel) + x
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 */
	public void deleteTiles(final int[] ids, final byte baseZoomInterval);

	/**
	 * Retrieves a tile's data as a byte array from the database. The data can be a vector
	 * representation or an image. If the tile does not exist null will be returned.
	 * 
	 * @param xPos
	 *            The tile's x coordinate in the grid for the given base zoom level.
	 * @param yPos
	 *            The tile's y coordinate in the grid for the given base zoom level.
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 * @return The tile as a byte array.
	 */
	public byte[] getTileData(final int xPos, final int yPos, final byte baseZoomInterval);

	/**
	 * Retrieves a tile's data as a byte array from the database. The data can be a vector
	 * representation or an image. If the tile does not exist null will be returned.
	 * 
	 * @param id
	 *            The tile's coordinate for the given base zoom level in a 1-dimensional representation:
	 *            id = (y * 4^baseZoomLevel) + x
	 * 
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 * 
	 * @return The tile as a byte array.
	 */
	public byte[] getTileData(final int id, final byte baseZoomInterval);

	/**
	 * Retrieves a set of tiles' data as a collection of byte arrays from the database. The data can be
	 * a vector representation or an image. If a tile does not exist null will be returned.
	 * 
	 * @param ids
	 *            The tile's coordinate for the given base zoom level in a 1-dimensional representation:
	 *            id = (y * 4^baseZoomLevel) + x
	 * 
	 * @param baseZoomInterval
	 *            The tile's base zoom level.
	 * 
	 * @return An collection of all tiles found.
	 */
	public Collection<TileDataContainer> getTileData(final int[] ids, final byte baseZoomInterval);

	/**
	 * Retrieves the map files metadata such as file version, bounding box, zoom interval configuration
	 * and more.
	 * 
	 * @return All base zoom levels <code>[bzl_0, ..., bzl_n]</code>.
	 */
	public MapFileMetaData getMetaData();

	/**
	 * Writes the given metadata to the database file. If the metadata needs to be upgraded use
	 * {@link #getMetaData()} as parameter.
	 * 
	 * @param mapFileMetaData
	 *            The metadata object containing the data to be written.
	 */
	public void setMetaData(MapFileMetaData mapFileMetaData);

	/**
	 * Closes the db.
	 */
	public void close();

}
