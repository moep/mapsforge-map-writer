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
package org.mapsforge.storage.dataExtraction;

import org.mapsforge.map.writer.model.GeoCoordinate;
import org.mapsforge.storage.tile.TileDataContainer;

/**
 * This class serves as a container for a map file's meta data.
 * 
 * @author Karsten Groll
 */
public class MapFileMetaData {

	static final String NL = "\r\n";

	private String fileVersion;
	private long dateOfCreation;

	// Bounding box
	private int minLat;
	private int minLon;
	private int maxLat;
	private int maxLon;

	private int tileSize;
	private String projection;
	private String languagePreference;
	private byte flags;

	// Map start position
	private int mapStartLon;
	private int mapStartLat;
	private byte startZoomLevel;

	private String comment;

	// POI tag mapping
	private int amountOfPOIMappings;
	private String[] poiMappings;

	// Ways tag mapping
	private int amountOfWayTagMappings;
	private String[] wayTagMappings;

	// Zoom interval configuration
	private byte amountOfZoomIntervals;
	private byte[] baseZoomLevel;
	private byte[] minimalZoomLevel;
	private byte[] maximalZoomLevel;
	private byte[] tileType;

	/**
	 * Creates a meta data object initialized with default values.
	 * 
	 * @return Meta data object initialized with default values;
	 */
	public static MapFileMetaData createInstanceWithDefaultValues() {
		MapFileMetaData ret = new MapFileMetaData();
		ret.setFileVersion("0.4-experimental");
		ret.setDateOfCreation(System.currentTimeMillis());
		ret.setBoundingBox(-90 * (int) GeoCoordinate.FACTOR_DOUBLE_TO_INT, -180
				* (int) GeoCoordinate.FACTOR_DOUBLE_TO_INT, 90 * (int) GeoCoordinate.FACTOR_DOUBLE_TO_INT,
				180 * (int) GeoCoordinate.FACTOR_DOUBLE_TO_INT);
		ret.setTileSize(256);
		ret.setProjection("Mercator");
		ret.setComment("Default metadata");

		ret.setAmountOfZoomIntervals((byte) 2);
		ret.prepareZoomIntervalConfiguration();
		ret.setZoomIntervalConfiguration(0, (byte) 8, (byte) 0, (byte) 11, TileDataContainer.TILE_TYPE_VECTOR);
		ret.setZoomIntervalConfiguration(1, (byte) 14, (byte) 12, (byte) 21, TileDataContainer.TILE_TYPE_VECTOR);

		return ret;
	}

	/**
	 * The constructor.
	 */
	public MapFileMetaData() {
	}

	/**
	 * @return true if the map is in debug mode.
	 */
	public boolean isDebugFlagSet() {
		return (this.flags & 0x80) != 0;
	}

	/**
	 * @return true if the map has map start position data.
	 */
	public boolean isMapStartPositionFlagSet() {
		return (this.flags & 0x40) != 0;
	}

	public boolean isStartZoomLevelFlagSet() {
		return (this.flags & 0x20) != 0;
	}

	/**
	 * This method prepares the array that contains the mappings for tag IDs to tag names for POIs.
	 */
	public void preparePOIMappings() {
		this.poiMappings = new String[this.amountOfPOIMappings];
	}

	/**
	 * This method prepares the array that contains the mappings for tag IDs to tag names for ways.
	 */
	public void prepareWayTagMappings() {
		this.wayTagMappings = new String[this.amountOfWayTagMappings];
	}

	/**
	 * This method prepares the arrays containing information about the base zoom level, minimal and maximal zoom level,
	 * the absolute start position and the subfile size.
	 */
	public void prepareZoomIntervalConfiguration() {
		this.baseZoomLevel = new byte[this.amountOfZoomIntervals];
		this.minimalZoomLevel = new byte[this.amountOfZoomIntervals];
		this.maximalZoomLevel = new byte[this.amountOfZoomIntervals];
		this.tileType = new byte[this.amountOfZoomIntervals];
	}

	public String getFileVersion() {
		return fileVersion;
	}

	public void setFileVersion(String fileVersion) {
		this.fileVersion = fileVersion;
	}

	public long getDateOfCreation() {
		return dateOfCreation;
	}

	public void setDateOfCreation(long dateOfCreation) {
		this.dateOfCreation = dateOfCreation;
	}

	public void setBoundingBox(int minLat, int minLon, int maxLat, int maxLon) {
		this.maxLat = maxLat;
		this.minLon = minLon;
		this.minLat = minLat;
		this.maxLon = maxLon;
	}

	public int getMinLat() {
		return minLat;
	}

	public int getMinLon() {
		return minLon;
	}

	public int getMaxLat() {
		return maxLat;
	}

	public int getMaxLon() {
		return maxLon;
	}

	public int getTileSize() {
		return tileSize;
	}

	public void setTileSize(int tileSize) {
		this.tileSize = tileSize;
	}

	public String getProjection() {
		return projection;
	}

	public void setProjection(String projection) {
		this.projection = projection;
	}

	public String getLanguagePreference() {
		return this.languagePreference;
	}

	public void setLanguagePreference(String languagePreference) {
		this.languagePreference = languagePreference;
	}

	public byte getFlags() {
		return flags;
	}

	public void setFlags(byte flags) {
		this.flags = flags;
	}

	public int getMapStartLon() {
		return mapStartLon;
	}

	public int getMapStartLat() {
		return mapStartLat;
	}

	public void setMapStartPosition(int lat, int lon) {
		this.mapStartLat = lat;
		this.mapStartLon = lon;
	}

	public byte getStartZoomLevel() {
		return this.startZoomLevel;
	}

	public void setStartZoomLevel(byte zoomLevel) {
		this.startZoomLevel = zoomLevel;
	}

	/**
	 * @return the comment
	 */
	public String getComment() {
		return comment;
	}

	/**
	 * @param comment
	 *            the comment to set
	 */
	public void setComment(String comment) {
		this.comment = comment;
	}

	/**
	 * @return the amountOfPOIMappings
	 */
	public int getAmountOfPOIMappings() {
		return amountOfPOIMappings;
	}

	/**
	 * @param amountOfPOIMappings
	 *            the amountOfPOIMappings to set
	 */
	public void setAmountOfPOIMappings(int amountOfPOIMappings) {
		this.amountOfPOIMappings = amountOfPOIMappings;
	}

	/**
	 * @return the pOIMappings
	 */
	public String[] getPOIMappings() {
		return poiMappings;
	}

	/**
	 * @param poiMappings
	 *            the pOIMappings to set
	 */
	public void setPOIMappings(String[] poiMappings) {
		this.poiMappings = poiMappings;
	}

	/**
	 * @return the amountOfWayTagMappings
	 */
	public int getAmountOfWayTagMappings() {
		return amountOfWayTagMappings;
	}

	/**
	 * @param amountOfWayTagMappings
	 *            the amountOfWayTagMappings to set
	 */
	public void setAmountOfWayTagMappings(int amountOfWayTagMappings) {
		this.amountOfWayTagMappings = amountOfWayTagMappings;
	}

	/**
	 * @return the wayTagMappings
	 */
	public String[] getWayTagMappings() {
		return wayTagMappings;
	}

	/**
	 * @param wayTagMappings
	 *            the wayTagMappings to set
	 */
	public void setWayTagMappings(String[] wayTagMappings) {
		this.wayTagMappings = wayTagMappings;
	}

	public byte getAmountOfZoomIntervals() {
		return amountOfZoomIntervals;
	}

	public void setAmountOfZoomIntervals(byte amountOfZoomIntervals) {
		this.amountOfZoomIntervals = amountOfZoomIntervals;
	}

	public void setZoomIntervalConfiguration(int zoomInterval, byte baseZoomLevel, byte minimalZoomLevel,
			byte maximalZoomLevel, byte tileType) {
		this.baseZoomLevel[zoomInterval] = baseZoomLevel;
		this.minimalZoomLevel[zoomInterval] = minimalZoomLevel;
		this.maximalZoomLevel[zoomInterval] = maximalZoomLevel;
		this.tileType[zoomInterval] = tileType;
	}

	public byte[] getBaseZoomLevel() {
		return this.baseZoomLevel;
	}

	public byte[] getMinimalZoomLevel() {
		return minimalZoomLevel;
	}

	public byte[] getMaximalZoomLevel() {
		return maximalZoomLevel;
	}

}
