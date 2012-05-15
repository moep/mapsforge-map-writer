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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Vector;

import org.mapsforge.map.writer.model.Rect;
import org.mapsforge.storage.dataExtraction.MapFileMetaData;

/**
 * An implementation that provides methods for accessing a map database on a PC
 * using SQLite3. This class is not thread safe and should therefore not be used
 * more than once at a time.
 * 
 * @author Karsten Groll
 */
public class PCTilePersistenceManager implements TilePersistenceManager {
	private String path;

	// Database
	private Connection conn = null;
	private Statement stmt = null;
	private PreparedStatement insertOrUpdateTileByIDStmt[] = null;
	private PreparedStatement deleteTileByIDStmt[] = null;
	private PreparedStatement getTileByIDStmt[] = null;
	private PreparedStatement getMetaDataStatement = null;
	private PreparedStatement insertOrUpdateMetaDataStatement = null;
	private ResultSet resultSet = null;

	private MapFileMetaData mapFileMetaData = null;

	/**
	 * Open the specified map database. If the database does not exist it will
	 * be created. You have to call {@link #init()} in order to perform
	 * operations on this database.
	 * 
	 * @param path
	 *            Path to a map database file.
	 * @param mfm
	 *            The map file's meta data. This will only be used when a new
	 *            map file should be created. Otherwise the meta data will be
	 *            parsed from the map file. If set to null, an empty meta data
	 *            container will be used for creating the database.
	 */
	public PCTilePersistenceManager(String path, MapFileMetaData mfm) {
		// TODO Throw FileNotFoundException
		this.path = path;
		if (mfm == null) {
			// Create default metadata values
			this.mapFileMetaData = MapFileMetaData
					.createInstanceWithDefaultValues();
		} else {
			this.mapFileMetaData = mfm;
		}
	}

	/**
	 * Open the specified map database. If the database does not exist it will
	 * be created. You have to call {@link #init()} in order to perform
	 * operations on this database.
	 * 
	 * @param path
	 *            Path to a map database file.
	 */
	public PCTilePersistenceManager(String path) {
		this(path, null);
	}

	/**
	 * Opens and creates the database and creates metadata tables.
	 */
	public void init() {
		System.out.println("Database has been initialized");
		// TODO Can this wrapper be merged with openOrCreateDB()?
		try {
			openOrCreateDB();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void openOrCreateDB() throws ClassNotFoundException, SQLException {
		// Own driver
		// Class.forName("SQLite.JDBC");
		// Xerial Driver
		Class.forName("org.sqlite.JDBC");

		this.conn = DriverManager.getConnection("jdbc:sqlite:/" + this.path);
		this.conn.setAutoCommit(false);
		this.stmt = this.conn.createStatement();

		// Create database if it does not yet exist.
		File dbFile = new File(this.path);
		if (dbFile.length() == 0) {
			createDatabase();
			initializePrivateStatements();
		} else {
			initializePrivateStatements();
			readMetaDataFromDB();
		}

		initializePrivateStatements();

	}

	private void initializePrivateStatements() throws SQLException {
		this.getMetaDataStatement = this.conn
				.prepareStatement("SELECT value FROM metadata WHERE key == ?;");
		this.insertOrUpdateMetaDataStatement = this.conn
				.prepareStatement("INSERT OR REPLACE INTO metadata VALUES(?, ?)");

		// Delete tile by ID statements
		this.deleteTileByIDStmt = new PreparedStatement[this.mapFileMetaData
				.getAmountOfZoomIntervals()];
		for (int i = 0; i < this.mapFileMetaData.getAmountOfZoomIntervals(); i++) {
			try {
				this.deleteTileByIDStmt[i] = this.conn
						.prepareStatement("DELETE FROM tiles_" + i
								+ " WHERE id == ?;");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Insert or update tile by ID statements
		this.insertOrUpdateTileByIDStmt = new PreparedStatement[this.mapFileMetaData
				.getAmountOfZoomIntervals()];
		for (int i = 0; i < this.mapFileMetaData.getAmountOfZoomIntervals(); i++) {
			try {
				this.insertOrUpdateTileByIDStmt[i] = this.conn
						.prepareStatement("INSERT OR REPLACE INTO tiles_" + i
								+ " VALUES (?,?);");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Get tile by ID statements
		this.getTileByIDStmt = new PreparedStatement[this.mapFileMetaData
				.getAmountOfZoomIntervals()];
		for (int i = 0; i < this.mapFileMetaData.getAmountOfZoomIntervals(); i++) {
			try {
				this.conn.prepareStatement("SELECT data FROM tiles_" + i
						+ " WHERE id == ?;");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void createDatabase() throws SQLException {
		System.out.println("Creating database");

		// CREATE TABLES
		for (int i = 0; i < this.mapFileMetaData.getAmountOfZoomIntervals(); i++) {
			this.stmt.executeUpdate("CREATE TABLE IF NOT EXISTS tiles_" + i
					+ " (id INTEGER, data BLOB, PRIMARY KEY (id));");
		}

		// Metadata (mostly information from former file header)
		this.stmt
				.executeUpdate("CREATE TABLE IF NOT EXISTS metadata (key STRING, value STRING, PRIMARY KEY (key));");
		this.stmt
				.executeUpdate("CREATE TABLE IF NOT EXISTS poi_tags (tag STRING, value INTEGER, PRIMARY KEY (value));");
		this.stmt
				.executeUpdate("CREATE TABLE IF NOT EXISTS way_tags (tag STRING, value INTEGER, PRIMARY KEY (value));");
		this.stmt
				.executeUpdate("CREATE TABLE IF NOT EXISTS zoom_interval_configuration "
						+ "(interval TINYINT, baseZoomLevel TINYINT, minimalZoomLevel TINYINT, maximalZoomLevel TINYINT, dataType TINYINT);");

		writeMetaDataToDB();

		// These values should only be added once and are not yet changeable

		// Create default zoom level configuration
		for (int i = 0; i < this.mapFileMetaData.getAmountOfZoomIntervals(); i++) {
			this.stmt
					.executeUpdate("INSERT INTO zoom_interval_configuration VALUES ('"
							+ i
							+ "','"
							+ this.mapFileMetaData.getBaseZoomLevel()[i]
							+ "','"
							+ this.mapFileMetaData.getMinimalZoomLevel()[i]
							+ "','"
							+ this.mapFileMetaData.getMaximalZoomLevel()[i]
							+ "','"
							+ TileDataContainer.TILE_TYPE_VECTOR
							+ "');");
		}

		// Create POI tag mapping entries
		for (int i = 0; i < this.mapFileMetaData.getAmountOfPOIMappings(); i++) {
			this.stmt.execute("INSERT INTO poi_tags (tag, value) VALUES ('"
					+ this.mapFileMetaData.getPOIMappings()[i] + "', '" + i
					+ "')");
		}

		// Create Way tag mapping entries
		for (int i = 0; i < this.mapFileMetaData.getAmountOfPOIMappings(); i++) {
			this.stmt.execute("INSERT INTO way_tags (tag, value) VALUES ('"
					+ this.mapFileMetaData.getWayTagMappings()[i] + "', '" + i
					+ "')");
		}

	}

	@Override
	public void insertOrUpdateTile(byte[] rawData, int xPos, int yPos,
			byte baseZoomInterval) {
		insertOrUpdateTile(rawData,
				coordinatesToID(xPos, yPos, baseZoomInterval), baseZoomInterval);
	}

	@Override
	public void insertOrUpdateTile(byte[] rawData, int id, byte baseZoomInterval) {
		try {
			this.insertOrUpdateTileByIDStmt[baseZoomInterval].setInt(1, id);
			this.insertOrUpdateTileByIDStmt[baseZoomInterval].setBytes(2,
					rawData);

			this.insertOrUpdateTileByIDStmt[baseZoomInterval].execute();
			this.conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void insertOrUpdateTiles(Collection<TileDataContainer> rawData) {
		int baseZoomLevel = rawData.iterator().next().getBaseZoomLevel();

		try {
			this.insertOrUpdateTileByIDStmt[baseZoomLevel].clearBatch();
			for (TileDataContainer tile : rawData) {
				this.insertOrUpdateTileByIDStmt[baseZoomLevel].setInt(
						1,
						coordinatesToID(tile.getxPos(), tile.getyPos(),
								tile.getBaseZoomLevel()));
				this.insertOrUpdateTileByIDStmt[baseZoomLevel].setBytes(2,
						tile.getData());
				this.insertOrUpdateTileByIDStmt[baseZoomLevel].addBatch();
			}

			this.insertOrUpdateTileByIDStmt[baseZoomLevel].executeBatch();
			this.conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void deleteTile(int xPos, int yPos, byte baseZoomInterval) {
		deleteTile(coordinatesToID(xPos, yPos, baseZoomInterval),
				baseZoomInterval);
	}

	@Override
	public void deleteTile(int id, byte baseZoomInterval) {
		try {
			this.deleteTileByIDStmt[baseZoomInterval].clearBatch();
			this.deleteTileByIDStmt[baseZoomInterval].setInt(1, id);

			this.deleteTileByIDStmt[baseZoomInterval].addBatch();
			this.deleteTileByIDStmt[baseZoomInterval].executeBatch();
			this.conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void deleteTiles(int[] id, byte baseZoomInterval) {
		try {
			this.deleteTileByIDStmt[baseZoomInterval].clearBatch();
			for (int i = 0; i < id.length; i++) {
				this.deleteTileByIDStmt[baseZoomInterval].setInt(1, id[i]);

				this.deleteTileByIDStmt[baseZoomInterval].addBatch();
			}
			this.deleteTileByIDStmt[baseZoomInterval].executeBatch();
			this.conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public byte[] getTileData(int xPos, int yPos, byte baseZoomInterval) {
		return getTileData(coordinatesToID(xPos, yPos, baseZoomInterval),
				baseZoomInterval);
	}

	@Override
	public byte[] getTileData(int id, byte baseZoomInterval) {
		byte[] result = null;

		try {
			this.getTileByIDStmt[baseZoomInterval].setInt(1, id);
			this.resultSet = this.getTileByIDStmt[baseZoomInterval]
					.executeQuery();

			if (this.resultSet.next()) {
				result = this.resultSet.getBytes(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return result;
	}

	@Override
	public Collection<TileDataContainer> getTileData(int[] ids,
			byte baseZoomInterval) {
		Vector<TileDataContainer> ret = new Vector<TileDataContainer>();

		// System.out.println("SELECT data FROM tiles_" + baseZoomLevel +
		// " WHERE id IN " +
		// getIDListString(ids) + ";");
		// TODO Can we use a prepared statement here?
		// TODO Set tile coordinates
		try {
			this.stmt.execute("SELECT * FROM tiles_" + baseZoomInterval
					+ " WHERE id IN (" + getIDListString(ids) + ");");
			this.resultSet = this.stmt.getResultSet();

			while (this.resultSet.next()) {
				// TODO calculate values (create constructor with id?)
				ret.add(new TileDataContainer(this.resultSet.getBytes(1),
						TileDataContainer.TILE_TYPE_VECTOR, -1, -1,
						baseZoomInterval));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ret;

	}

	private static String getIDListString(int ids[]) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < ids.length; i++) {
			sb.append(ids[i]);

			if (i != ids.length - 1) {
				sb.append(',');
			}
		}

		return sb.toString();
	}

	@Override
	public MapFileMetaData getMetaData() {
		return this.mapFileMetaData;
	}

	@Override
	public void setMetaData(MapFileMetaData metaData) {
		this.mapFileMetaData = metaData;
		writeMetaDataToDB();
	}

	/**
	 * This synchronizes the metadata object with the DB. Entries in the
	 * database will be updated. Keys that do not yet exist will be created.
	 */
	private void writeMetaDataToDB() {
		System.out.println("Writing meta data");
		try {
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('version', '"
							+ this.mapFileMetaData.getFileVersion() + "');");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('dateOfCreation', '"
							+ this.mapFileMetaData.getDateOfCreation() + "');");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('boundingBoxMinLat', "
							+ this.mapFileMetaData.getMinLat() + " );");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('boundingBoxMaxLat', "
							+ this.mapFileMetaData.getMaxLat() + " );");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('boundingBoxMinLon', "
							+ this.mapFileMetaData.getMinLon() + " );");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('boundingBoxMaxLon', "
							+ this.mapFileMetaData.getMaxLon() + " );");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('tileSize', '"
							+ this.mapFileMetaData.getTileSize() + "');");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('projection', '"
							+ this.mapFileMetaData.getProjection() + "');");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('languagePreference', '"
							+ this.mapFileMetaData.getLanguagePreference()
							+ "');");

			// Flags
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('debugInformationFlag', '"
							+ (this.mapFileMetaData.isDebugFlagSet() ? "1"
									: "0") + "');");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('mapPositionExistsFlag', '"
							+ (this.mapFileMetaData.isMapStartPositionFlagSet() ? "1"
									: "0") + "');");
			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('startZoomLevelExistsFlag', '"
							+ (this.mapFileMetaData.isStartZoomLevelFlagSet() ? "1"
									: "0") + "');");

			if (this.mapFileMetaData.isMapStartPositionFlagSet()) {
				this.stmt
						.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('mapStartLat', '"
								+ this.mapFileMetaData.getMapStartLat() + "');");
				this.stmt
						.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('mapStartLon', '"
								+ this.mapFileMetaData.getMapStartLon() + "');");
			}

			if (this.mapFileMetaData.isStartZoomLevelFlagSet()) {
				this.stmt
						.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('startZoomLevel', '"
								+ this.mapFileMetaData.getStartZoomLevel()
								+ "');");
			}

			this.stmt
					.executeUpdate("INSERT OR REPLACE INTO metadata VALUES ('comment', '"
							+ this.mapFileMetaData.getComment() + "');");
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private void readMetaDataFromDB() {
		this.mapFileMetaData = new MapFileMetaData();

		try {
			// Version
			this.getMetaDataStatement.setString(1, "version");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				this.mapFileMetaData
						.setFileVersion(this.resultSet.getString(1));
			}

			// Date of creation
			this.getMetaDataStatement.setString(1, "dateOfCreation");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				this.mapFileMetaData.setDateOfCreation(Long
						.parseLong(this.resultSet.getString(1)));
			}

			// Bounding box
			Rect boundingBox = new Rect(0, 0, 0, 0);
			this.getMetaDataStatement.setString(1, "boundingBoxMinLat");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				boundingBox.minLatitudeE6 = Integer.parseInt(this.resultSet
						.getString(1));
			}
			this.getMetaDataStatement.setString(1, "boundingBoxMaxLat");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				boundingBox.maxLatitudeE6 = Integer.parseInt(this.resultSet
						.getString(1));
			}
			this.getMetaDataStatement.setString(1, "boundingBoxMinLon");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				boundingBox.minLongitudeE6 = Integer.parseInt(this.resultSet
						.getString(1));
			}
			this.getMetaDataStatement.setString(1, "boundingBoxMaxLon");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				boundingBox.maxLongitudeE6 = Integer.parseInt(this.resultSet
						.getString(1));
			}
			this.mapFileMetaData.setBoundingBox(boundingBox.minLatitudeE6,
					boundingBox.minLongitudeE6, boundingBox.maxLatitudeE6,
					boundingBox.maxLongitudeE6);

			// Tile size
			this.getMetaDataStatement.setString(1, "tileSize");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				this.mapFileMetaData.setTileSize(Integer
						.parseInt(this.resultSet.getString(1)));
			}

			// Projection
			this.getMetaDataStatement.setString(1, "projection");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				this.mapFileMetaData.setProjection(this.resultSet.getString(1));
			}

			// Language preference
			this.getMetaDataStatement.setString(1, "languagePreference");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				this.mapFileMetaData.setLanguagePreference(this.resultSet
						.getString(1));
			}

			byte flags = 0;
			// Debug flag
			this.getMetaDataStatement.setString(1, "debugInformationFlag");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				if (this.resultSet.getString(1).equals("1")) {
					flags = (byte) (flags | (byte) 0x80);
				}
			}

			// Map position flag
			this.getMetaDataStatement.setString(1, "mapPositionExistsFlag");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				if (this.resultSet.getString(1).equals("1")) {
					flags = (byte) (flags | (byte) 0x40);
				}
			}

			// Start zoom level flag
			this.getMetaDataStatement.setString(1, "startZoomLevelExistsFlag");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				if (this.resultSet.getString(1).equals("1")) {
					flags = (byte) (flags | (byte) 0x20);
				}
			}

			this.mapFileMetaData.setFlags(flags);

			// Map start position
			int mapStartLat = 0;
			int mapStartLon = 0;
			if (this.mapFileMetaData.isMapStartPositionFlagSet()) {
				this.getMetaDataStatement.setString(1, "mapStartLat");
				this.getMetaDataStatement.execute();
				this.resultSet = this.getMetaDataStatement.getResultSet();
				if (this.resultSet.next()) {
					mapStartLat = Integer.parseInt(this.resultSet.getString(1));
				}
				this.getMetaDataStatement.setString(1, "mapStartLon");
				this.getMetaDataStatement.execute();
				this.resultSet = this.getMetaDataStatement.getResultSet();
				if (this.resultSet.next()) {
					mapStartLon = Integer.parseInt(this.resultSet.getString(1));
				}

				this.mapFileMetaData.setMapStartPosition(mapStartLat,
						mapStartLon);
			}

			// Start zoom level
			if (this.mapFileMetaData.isStartZoomLevelFlagSet()) {
				this.getMetaDataStatement.setString(1, "startZoomLevel");
				this.getMetaDataStatement.execute();
				this.resultSet = this.getMetaDataStatement.getResultSet();
				if (this.resultSet.next()) {
					this.mapFileMetaData.setStartZoomLevel(Byte
							.parseByte((this.resultSet.getString(1))));
				}
			}

			// Comment
			this.getMetaDataStatement.setString(1, "comment");
			this.getMetaDataStatement.execute();
			this.resultSet = this.getMetaDataStatement.getResultSet();
			if (this.resultSet.next()) {
				this.mapFileMetaData.setComment(this.resultSet.getString(1));
			}

			// POI tag mappings
			int numPoiTags = 0;
			this.stmt.execute("SELECT count(*) FROM poi_tags;");
			this.resultSet = this.stmt.getResultSet();
			while (this.resultSet.next()) {
				numPoiTags = Integer.parseInt(this.resultSet.getString(1));
			}
			this.mapFileMetaData.setAmountOfPOIMappings(numPoiTags);
			this.mapFileMetaData.preparePOIMappings();

			this.stmt.execute("SELECT tag, value FROM poi_tags;");
			this.resultSet = this.stmt.getResultSet();
			while (this.resultSet.next()) {
				this.mapFileMetaData.getPOIMappings()[this.resultSet.getInt(2)] = this.resultSet
						.getString(1);
			}

			// Way Tag mappings
			int numWayTags = 0;
			this.stmt.execute("SELECT count(*) FROM way_tags;");
			this.resultSet = this.stmt.getResultSet();
			while (this.resultSet.next()) {
				numWayTags = Integer.parseInt(this.resultSet.getString(1));
			}
			this.mapFileMetaData.setAmountOfWayTagMappings(numWayTags);
			this.mapFileMetaData.prepareWayTagMappings();

			this.stmt.execute("SELECT tag, value FROM way_tags;");
			this.resultSet = this.stmt.getResultSet();
			while (this.resultSet.next()) {
				this.mapFileMetaData.getWayTagMappings()[this.resultSet
						.getInt(2)] = this.resultSet.getString(1);
			}

			// Zoom interval configuration
			byte numIntervals = 0;
			this.stmt
					.execute("SELECT count(*) FROM zoom_interval_configuration;");
			this.resultSet = this.stmt.getResultSet();
			while (this.resultSet.next()) {
				numIntervals = Byte.parseByte(this.resultSet.getString(1));
			}
			this.mapFileMetaData.setAmountOfZoomIntervals(numIntervals);
			this.mapFileMetaData.prepareZoomIntervalConfiguration();

			this.stmt
					.execute("SELECT interval, baseZoomLevel, minimalZoomLevel, maximalZoomLevel, dataType FROM zoom_interval_configuration;");
			this.resultSet = this.stmt.getResultSet();
			while (this.resultSet.next()) {
				this.mapFileMetaData.setZoomIntervalConfiguration(
						Integer.parseInt(this.resultSet.getString(1)),
						Byte.parseByte(this.resultSet.getString(2)),
						Byte.parseByte(this.resultSet.getString(3)),
						Byte.parseByte(this.resultSet.getString(4)),
						Byte.parseByte(this.resultSet.getString(5)));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void close() {
		try {
			if (!this.conn.isClosed()) {
				this.conn.commit();
				this.conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private int coordinatesToID(int xPos, int yPos, int baseZoomInterval) {
		return (int) (yPos
				* Math.pow(
						2,
						this.mapFileMetaData.getBaseZoomLevel()[baseZoomInterval]) + xPos);
	}

	/**
	 * Main method for testing purposes.
	 * 
	 * @param args
	 *            Not used.
	 */
	public static void main(String[] args) {
		PCTilePersistenceManager tpm = new PCTilePersistenceManager(
				"/home/moep/maps/mapsforge/test.map");
		tpm.init();

		Vector<TileDataContainer> tiles = new Vector<TileDataContainer>();
		tiles.add(new TileDataContainer("moep".getBytes(),
				TileDataContainer.TILE_TYPE_VECTOR, 1, 0, (byte) 1));
		tiles.add(new TileDataContainer("bla".getBytes(),
				TileDataContainer.TILE_TYPE_VECTOR, 2, 0, (byte) 1));
		tiles.add(new TileDataContainer("blubb".getBytes(),
				TileDataContainer.TILE_TYPE_VECTOR, 3, 0, (byte) 1));
		tiles.add(new TileDataContainer("bleh!".getBytes(),
				TileDataContainer.TILE_TYPE_VECTOR, 4, 0, (byte) 1));
		tiles.add(new TileDataContainer("narf!".getBytes(),
				TileDataContainer.TILE_TYPE_VECTOR, 5, 0, (byte) 1));

		tpm.insertOrUpdateTiles(tiles);

		Collection<TileDataContainer> ret = tpm.getTileData(
				new int[] { 2, 3, 4 }, (byte) 1);
		for (TileDataContainer c : ret) {
			// This line only makes sense if the debug flag is set
			System.out.println(c.getData());
		}

		tpm.close();

	}
}
