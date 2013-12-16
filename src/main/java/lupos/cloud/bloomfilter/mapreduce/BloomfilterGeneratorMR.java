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
package lupos.cloud.bloomfilter.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import lupos.cloud.hbase.HBaseConnection;
import lupos.cloud.hbase.HBaseDistributionStrategy;

/**
 * * Mit Hilfe dieser Klasse wird die Byte-Bitvektorgeneierung gestartet. Die
 * Ausführung erfolgt über mehrere MapReduce-Jobs (für jede Tabelle einer).
 */
public class BloomfilterGeneratorMR {

	/**
	 * Fuer jeden Bitvektor mit einer Kardinalität von > MIN_CARD wird der
	 * Byte-Bitvektor erzeugt.
	 */
	public static Integer MIN_CARD = 25000;

	/**
	 * Beschreibt die maximale Anzahl der Key-Value Paare die pro
	 * scan.next()-Aufruf übertragen wird. Diese Zahl darf nicht zu groß sein,
	 * denn HBase lädt das gesamte Ergebnis in den Arbeitsspeicher.
	 */
	public static Integer BATCH = 5000;

	/** The caching. */
	public static Integer CACHING = 100;

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 * @throws InterruptedException
	 *             the interrupted exception
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		System.out.println("Starts with b: " + BATCH + " c: " + CACHING);
		HBaseConnection.init();
		ArrayList<BVJobThread> jobList = new ArrayList<BVJobThread>();

		long startTime = System.currentTimeMillis();

		String[] tables = HBaseDistributionStrategy.getTableInstance()
				.getTableNames();


		for (String tablename : tables) {
			System.out.println("Aktuelle Tabelle: " + tablename);
			BVJobThread curJob = new BVJobThread(tablename);
			jobList.add(curJob);
			curJob.start();

		}

		System.out.println("Warte bis alle Jobs abgeschlossen sind ...");
		for (BVJobThread job : jobList) {
			while (!job.isFinished()) {
				sleep(2000);
			}
			System.out.println(job.getTablename() + " ist fertig");
		}

		long stopTime = System.currentTimeMillis();
		System.out.println("Bitvektor Generierung beendet." + " Dauer: "
				+ (stopTime - startTime) / 1000 + "s");
	}

	/**
	 * Sleep.
	 * 
	 * @param time
	 *            the time
	 */
	private static void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
