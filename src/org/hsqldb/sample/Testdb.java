/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.hsqldb.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

/**
 * Title: Testdb Description: TPC-H test for HSQLDB
 *
 * Require all data prepared in prefix folder
 *
 * Author: Yoohee jlmhkwang@gmail.com
 */
public class Testdb {

	static String prefix = "/Users/Cyclops-THSS/Documents/2017Spring/计算机系统软件(2)/TPC-H/data/";
	static String[] tables = { "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem" };

	Connection conn; // our connnection to the db - presist for life of program

	long start;

	// we dont want this garbage collected until we are done
	public Testdb(String db_file_name_prefix) throws Exception {
		// Load the HSQL Database Engine JDBC driver
		Class.forName("org.hsqldb.jdbc.JDBCDriver");
		conn = DriverManager.getConnection("jdbc:hsqldb:" + db_file_name_prefix, // filenames
				"SA", // username
				""); // password
	}

	public void shutdown() throws SQLException {
		Statement st = conn.createStatement();
		st.execute("SHUTDOWN");
		conn.close();
	}

	// use for SQL command SELECT
	public synchronized void query(String expression, int id) throws SQLException {

		Statement st = null;
		ResultSet rs = null;

		st = conn.createStatement(); // statement objects can be reused with

		// repeated calls to execute but we
		// choose to make a new one each time
		rs = st.executeQuery(expression); // run the query

		// do something with the result set.
		dump(rs, id);
		st.close();
	}

	// use for SQL commands CREATE, DROP, INSERT and UPDATE
	public synchronized void update(String expression) throws SQLException {

		Statement st = null;

		st = conn.createStatement(); // statements

		int i = st.executeUpdate(expression); // run the query

		if (i == -1) {
			System.out.println("db error : " + expression);
		}

		st.close();
	} // void update()

	public static void dump(ResultSet rs, int id) throws SQLException {

		// the order of the rows in a cursor
		// are implementation dependent unless you use the SQL ORDER statement
		ResultSetMetaData meta = rs.getMetaData();
		int colmax = meta.getColumnCount();
		int i;
		Object o = null;

		// the result set is a cursor into the data. You can only
		// point to one row at a time
		// assume we are pointing to BEFORE the first row
		// rs.next() points to next row and returns true
		// or false if there is no next row, which breaks the loop
		try {
			PrintWriter writer = new PrintWriter("r" + id + ".txt", "UTF-8");
			for (; rs.next();) {
				for (i = 0; i < colmax; ++i) {
					o = rs.getObject(i + 1); // Is SQL the first column is
					// indexed with 1 not 0
					writer.print(o.toString() + " ");
				}
				writer.println(" ");
			}
			writer.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	} // void dump( ResultSet rs )

	public void load_lines(String file) throws Exception {
		InputStream is = new FileInputStream(prefix + file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		String str = null;
		while (true) {
			str = reader.readLine();
			if (str != null)
				update(str);
			else
				break;
		}
		is.close();
	} // void load_lines( String file )

	public void load_query(String file, int id) throws Exception {
		InputStream is = new FileInputStream(prefix + file);
		String str = new String(Files.readAllBytes(Paths.get(prefix + file)));
		query(str, id);
		is.close();
	} // void load_query( String file )

	public void load_update(String file) throws Exception {
		InputStream is = new FileInputStream(prefix + file);
		String str = new String(Files.readAllBytes(Paths.get(prefix + file)));
		update(str);
		is.close();
	} // void load_update( String file )

	public void settimer() {
		start = System.currentTimeMillis();
	}

	public void report(String info) {
		long sep = System.currentTimeMillis() - start;
		System.out.println(info + ":" + sep);
	}

	public static void main(String[] args) {

		Testdb db = null;

		try {
			db = new Testdb("test");
		} catch (Exception ex1) {
			ex1.printStackTrace(); // could not start db
			return; // bye bye
		}

		try {
			db.settimer();
			// Create table
			db.load_update("dss.ddl");
			db.load_lines("dss.ri");
			// Insert data
			for (String s : tables) {
				db.load_lines(s + ".sql");
			}
			// Load test finished
			db.report("Load");
			// RF1
			db.settimer();
			db.load_lines("RF1.sql");
			db.report("RF1");
			// Queries
			for (int i = 1; i < 23; i++) {
				if (i == 7 || i == 21)
					continue;
				if (i == 15) {
					db.load_update("q15.1.sql");
					db.load_query("q15.2.sql", i);
					db.load_update("q15.3.sql");
				} else {
					db.settimer();
					db.load_query("q" + i + ".sql", i);
				}
				db.report("Q" + i);
			}
			// RF2
			db.settimer();
			db.load_lines("RF2.sql");
			db.report("RF2");
		} catch (Exception ex2) {
			ex2.printStackTrace();
			return;
		}

		try {
			db.shutdown();
		} catch (SQLException ex3) {
			ex3.printStackTrace();
		}
	} // main()
} // class Testdb
