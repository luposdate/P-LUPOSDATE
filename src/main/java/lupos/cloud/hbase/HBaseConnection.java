/**
 * Copyright (c) 2013, Institute of Information Systems (Sven Groppe, Thomas Kiencke and contributors of P-LUPOSDATE), University of Luebeck
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 	- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * 	  disclaimer.
 * 	- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * 	  following disclaimer in the documentation and/or other materials provided with the distribution.
 * 	- Neither the name of the University of Luebeck nor the names of its contributors may be used to endorse or promote
 * 	  products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package lupos.cloud.hbase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import lupos.cloud.bloomfilter.BitvectorManager;
import lupos.cloud.hbase.bulkLoad.BulkLoad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * In erster Linie wird diese Klasse genutzt um die Verbindung mit HBase
 * herzustellen, die notwendigen Tabellen zu erzeugen und zum hinzufügen der
 * Rows. Beim Laden in HBase gibt es zwei Mögliche Modi, die mit der Variable
 * MAP_REDUCE_BULK_LOAD definiert werden. Bei größeren Datenmengen bietet sich
 * der "BulkLoad"-Modus an indem die Tripel erst lokal gecacht, und anschließend
 * per Map Reduce Job auf die verschiedenen Knoten verteilt werden.
 */
public class HBaseConnection {

	/** HBase/Hadoop Konfigurations-Objekt . */
	static Configuration configuration = null;

	/** HBase Schnittstelle. */
	static HBaseAdmin admin = null;

	/** Wenn true, weden INFO-Messages ausgegeben. */
	static boolean message = true;

	/** HTable-Referenzen. */
	static HashMap<String, HTable> hTables = new HashMap<String, HTable>();

	/** CSV-Writer Referenze. */
	static HashMap<String, CSVWriter> csvwriter = new HashMap<String, CSVWriter>();

	/** Zähler der gespeicherten HBase-Tripel. */
	static int rowCounter = 0;

	/** Anzahl der Zwischengespeicherten HBase-Tripel. */
	public static int ROW_BUFFER_SIZE = 21000000;

	/** Arbeitsverzeichnis. */
	public static final String WORKING_DIR = "bulkLoadDirectory";

	/** Arbeitsname der Datei. */
	public static final String BUFFER_FILE_NAME = "rowBufferFile";

	/** Arbeitsname des HFiles. */
	public static final String BUFFER_HFILE_NAME = "rowBufferHFile";

	/** HDFS-Referenz */
	static FileSystem hdfs_fileSystem = null;

	/** Wenn diese Variable True ist, ist der BulkLoad-Modus aktiv. */
	public static boolean MAP_REDUCE_BULK_LOAD = true;

	/** Wenn aktiv, werden die HBase-Tabellen beim Start gelöscht. */
	public static boolean deleteTableOnCreation = false;

	/**
	 * Initialisierung der Verbindung und erstellen der Arbeitsverzeichnisse auf
	 * dem verteilten Dateisystem.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void init() throws IOException {
		if (configuration == null || admin == null) {
			System.out.println("Verbindung wird aufgebaut ...");
			configuration = HBaseConfiguration.create();
			admin = new HBaseAdmin(configuration);
			System.out
					.print("Verbindung zum Cluster wurde hergestellt. Knoten: ");
			for (final ServerName serv : admin.getClusterStatus().getServers()) {
				System.out.print(serv.getHostname() + " ");
			}
			System.out.println();
		}

		if (MAP_REDUCE_BULK_LOAD == true && hdfs_fileSystem == null) {

			hdfs_fileSystem = FileSystem.get(configuration);
			hdfs_fileSystem.delete(new Path("/tmp/" + WORKING_DIR), true);
			hdfs_fileSystem.mkdirs(new Path("/tmp/" + WORKING_DIR));

			new File(WORKING_DIR).mkdir();

		}

	}

	/**
	 * Erzeugt für jeden Index eine Tabelle und erstellt die Column-Families +
	 * aktiviert LZO Komprimierung.
	 *
	 * @param tablename
	 *            the tablename
	 * @param familyname
	 *            the familyname
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void createTable(final String tablename, final String familyname)
			throws IOException {
		init();
		try {
			if (deleteTableOnCreation) {
				deleteTable(tablename);
			}
			final HTableDescriptor descriptor = new HTableDescriptor(
					Bytes.toBytes(tablename));
			final HColumnDescriptor family = new HColumnDescriptor(familyname);
			final HColumnDescriptor familyb1 = new HColumnDescriptor(
					BitvectorManager.bloomfilter1ColumnFamily);
			final HColumnDescriptor familyb2 = new HColumnDescriptor(
					BitvectorManager.bloomfilter2ColumnFamily);
			family.setCompressionType(Algorithm.LZO);
			familyb1.setCompressionType(Algorithm.LZO);
			familyb2.setCompressionType(Algorithm.LZO);
			descriptor.addFamily(family);
			descriptor.addFamily(familyb1);
			descriptor.addFamily(familyb2);
			admin.createTable(descriptor);
			if (message) {
				System.out.println("Tabelle \"" + tablename
						+ "\" wurde erzeugt");
			}
		} catch (final TableExistsException e) {
			if (message) {
				System.out.println("Tabelle \"" + tablename
						+ "\" existiert bereits!");
			}
		}
	}

	/**
	 * Die Tripel in dem lokalen TripelCache werden in HBase geladen (nur für
	 * den BulkLoad).
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void flush() throws IOException {
		if (rowCounter > 0) {
			startBulkLoad();
		}
	}

	/**
	 * Fügt eine Spalte zu einer Tabelle hinzu.
	 *
	 * @param tablename
	 *            the tablename
	 * @param columnname
	 *            the columnname
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void addColumn(final String tablename, final String columnname)
			throws IOException {
		init();
		if (!checkTable(tablename)) {
			return;
		}

		admin.addColumn(tablename, new HColumnDescriptor(columnname));
		if (message) {
			System.out.println("Spalte \"" + columnname
					+ "\" wurde in die Tabelle \"" + tablename
					+ "\" hinzugefügt!");
		}
	}

	/**
	 * Entfernt ein HBase Triple.
	 *
	 * @param tablename
	 *            the tablename
	 * @param columnFamily
	 *            the column family
	 * @param rowKey
	 *            the row key
	 * @param colunmnname
	 *            the colunmnname
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void deleteRow(final String tablename, final String columnFamily,
			final String rowKey, final String colunmnname) throws IOException {
		init();
		HTable table = hTables.get(tablename);
		if (table == null) {
			table = new HTable(configuration, tablename);
			hTables.put(tablename, table);
		}
		final Delete row = new Delete(rowKey.getBytes());
		row.deleteColumn(columnFamily.getBytes(), colunmnname.getBytes());
		table.delete(row);
		if (message) {
			System.out.println(rowKey + " und Spalte " + colunmnname
					+ " wurden geloescht");
		}
	}

	/**
	 * Deaktivieren einer Tabelle.
	 *
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void disableTable(final String tablename) throws IOException {
		init();
		if (!checkTable(tablename)) {
			return;
		}

		admin.disableTable(tablename);
		if (message) {
			System.out.println("Tabelle \"" + tablename
					+ "\" wurde deaktiviert");
		}
	}

	/**
	 * Aktivieren einer Tabelle.
	 *
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void enableTable(final String tablename) throws IOException {
		init();
		if (!checkTable(tablename)) {
			return;
		}

		admin.enableTable(tablename);
		if (message) {
			System.out.println("Tabelle \"" + tablename + "\" wurde aktiviert");
		}
	}

	/**
	 * Löschen einer Tabelle. Dazu wird sie erste deaktiviert und anschließend
	 * gelöscht.
	 *
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void deleteTable(final String tablename) throws IOException {
		init();
		if (!checkTable(tablename)) {
			return;
		}

		try {
			admin.disableTable(tablename);
		} catch (final TableNotEnabledException e) {
			// ignore
		}
		admin.deleteTable(tablename);
		if (message) {
			System.out.println("Tabelle \"" + tablename + "\" wurde gelöscht");
		}
	}

	/**
	 * Gibt alle Tabellen zurück.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void listAllTables() throws IOException {
		init();
		final HTableDescriptor[] list = admin.listTables();
		System.out.println("Tabellen (Anzahl = " + list.length + "):");
		for (final HTableDescriptor table : list) {
			System.out.println(table.toString());
		}
	}

	/**
	 * Prüft ob eine Tabelle verfügbar ist.
	 *
	 * @param tablename
	 *            the tablename
	 * @return true, if successful
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static boolean checkTable(final String tablename) throws IOException {
		init();
		if (!admin.isTableAvailable(tablename)) {
			System.out
					.println("Tabelle \"" + tablename + "\" nicht verfügbar!");
			return false;
		}
		return true;
	}

	/**
	 * Fügt eine Reihe (=row) hinzu.
	 *
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @param columnFamily
	 *            the column family
	 * @param column
	 *            the column
	 * @param value
	 *            the value
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void addRow(final String tablename, final String row_key,
			final String columnFamily, final String column, final String value)
			throws IOException {
		// schnellere Variante zum einlesen von Tripel
		if (MAP_REDUCE_BULK_LOAD) {
			if (csvwriter.get(tablename) == null) {
				csvwriter.put(tablename, new CSVWriter(new FileWriter(
						WORKING_DIR + File.separator + tablename + "_"
								+ BUFFER_FILE_NAME + ".csv"), '\t'));
			}
			// Schreibe die Zeile in auf den Festplattenpuffer
			final String[] entries = { columnFamily, row_key, column, value };
			csvwriter.get(tablename).writeNext(entries);
			rowCounter++;

			if (rowCounter == ROW_BUFFER_SIZE) {
				startBulkLoad();
			}

		} else {
			// Einlesen per Tripel per HBase API (sehr langsam bei großen
			// Datenmengen), da die Tripel einzeln übertragen weden.
			HTable table = hTables.get(tablename);
			if (table == null) {
				table = new HTable(configuration, tablename);
				hTables.put(tablename, table);
			}
			final Put row = new Put(Bytes.toBytes(row_key));
			row.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
					Bytes.toBytes(value));

			final String toSplit = column;
			String elem1 = null;
			String elem2 = null;
			if (toSplit.contains(",")) {
				elem1 = toSplit.substring(0, toSplit.indexOf(","));
				elem2 = toSplit.substring(toSplit.indexOf(",") + 1,
						toSplit.length());
			} else {
				elem1 = toSplit.substring(0, toSplit.length());
			}
			// Bloomfilter
			if (!(elem1 == null)) {
				final Integer position = BitvectorManager.hash(elem1.getBytes());
				row.add(BitvectorManager.bloomfilter1ColumnFamily,
						integerToByteArray(4, position), Bytes.toBytes(""));
			}

			if (!(elem2 == null)) {
				final Integer position = BitvectorManager.hash(elem2.getBytes());
				row.add(BitvectorManager.bloomfilter2ColumnFamily,
						integerToByteArray(4, position), Bytes.toBytes(""));
			}

			table.put(row);

		}
	}

	/**
	 * Integer to byte array.
	 *
	 * @param allocate
	 *            the allocate
	 * @param pos
	 *            the pos
	 * @return the byte[]
	 */
	public static byte[] integerToByteArray(final int allocate, final Integer pos) {
		return ByteBuffer.allocate(allocate).putInt(pos).array();
	}

	/**
	 * Start bulk load.
	 *
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void startBulkLoad() throws IOException {
		rowCounter = 0;
		final ArrayList<BulkLoad> bulkList = new ArrayList<BulkLoad>();
		for (final String key : csvwriter.keySet()) {
			csvwriter.get(key).close();
			hdfs_fileSystem.copyFromLocalFile(true, true, new Path(WORKING_DIR
					+ File.separator + key + "_" + BUFFER_FILE_NAME + ".csv"),
					new Path("/tmp/" + WORKING_DIR + "/" + key + "_"
							+ BUFFER_FILE_NAME + ".csv"));
			final BulkLoad b = new BulkLoad(key);
			b.start();
			bulkList.add(b);
		}

		System.out.println("Wait until jobs are finished ...");
		boolean allJobsReady = false;
		while (!allJobsReady) {
			allJobsReady = true;
			for (final BulkLoad b : bulkList) {
				if (!b.isFinished()) {
					allJobsReady = false;
				}
			}
			wait(5); // warte 5 sekunden
		}
		System.out.println("ready!");

		csvwriter = new HashMap<String, CSVWriter>();
		hdfs_fileSystem.delete(new Path("/tmp/" + WORKING_DIR), true);
		hdfs_fileSystem.mkdirs(new Path("/tmp/" + WORKING_DIR));
	}

	/**
	 * Wait.
	 *
	 * @param sec
	 *            the sec
	 */
	public static void wait(final int sec) {
		try {
			Thread.sleep(sec * 1000);
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gibt eine Zeile anhand des rowKeys zurück.
	 *
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @return the row
	 */
	public static Result getRow(final String tablename, final String row_key) {
		try {
			init();
			HTable table = hTables.get(tablename);
			if (table == null) {
				table = new HTable(configuration, tablename);
				hTables.put(tablename, table);
			}
			final Get g = new Get(Bytes.toBytes(row_key));
			final Result result = table.get(g);
			if (result != null) {
				return result;
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Gibt eine Zeile anhand des rowkeys und den Prefix einer Spalte zurück.
	 *
	 * @param tablename
	 *            the tablename
	 * @param row_key
	 *            the row_key
	 * @param cf
	 *            the cf
	 * @return the row with column
	 */
	public static Result getRowWithColumn(final String tablename,
			final String row_key, final String cf) {
		try {
			init();
			HTable table = hTables.get(tablename);
			if (table == null) {
				table = new HTable(configuration, tablename);
				hTables.put(tablename, table);
			}
			final Get g = new Get(Bytes.toBytes(row_key));
			g.addFamily(cf.getBytes());
			// g.setFilter(new ColumnPrefixFilter(column.getBytes()));
			final Result result = table.get(g);
			if (result != null) {
				return result;
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Gibt eine Tabelle aus.
	 *
	 * @param tablename
	 *            the tablename
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void printTable(final String tablename) throws IOException {
		init();
		final HTable table = new HTable(configuration, tablename);
		final ResultScanner scanner = table.getScanner(new Scan());
		try {
			for (final Result scannerResult : scanner) {
				System.out.println("row_key: " + scannerResult.getRow());

			}
		} finally {
			scanner.close();
			table.close();
		}
	}

	/**
	 * Gibt das Konfigurationsobjekt zurück.
	 *
	 * @return the configuration
	 */
	public static Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Gets the hdfs_file system.
	 *
	 * @return the hdfs_file system
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static FileSystem getHdfs_fileSystem() throws IOException {
		if (hdfs_fileSystem == null) {
			hdfs_fileSystem = FileSystem.get(configuration);
		}
		return hdfs_fileSystem;
	}
}
