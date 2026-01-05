
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/*
 * 一个连接中，使用不同的绑定变量，校验执行结果是否正确
 */
public class ExecChecker extends Thread {
	private static String url = "jdbc:postgresql://10.44.133.56:5432/pg";
	private static String userName = "bank";
	private static String password = "gs_123456";
	private static String dbType = "postgres";

	public ExecChecker(String svt) {
		if (svt == null) {
			System.out.println("invalid parameter");
			return;
		}

		if (svt.equalsIgnoreCase("gaussdb")) {
			url = "jdbc:opengauss://10.145.255.156:30100,10.145.255.158:30100,10.145.255.160:30100/test?targetServerType=master&prepareThreshold=5&batchMode=on&fetchsize=2&loggerLevel=off&loggerFile=./gsjdbc.log";
			dbType = "gaussdb";
		} else if (svt.equalsIgnoreCase("pg")) {
			url = "jdbc:postgresql://10.44.133.56:5432/pg";
			dbType = "postgres";
		} else {
			System.out.println("invalid dbtype");
			return;
		}
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("usage: url userName password");
			return;
		}
		url = args[0];
		userName = args[1];
		password = args[2];

		try {
			for (int i = 0; i < 500; i++) {
				new ExecChecker("gaussdb").start();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	Connection conn1 = null;
	PreparedStatement stmt1 = null;
	PreparedStatement stmt2 = null;
	Statement stmt = null;

	private boolean initDB() {
		do {
			conn1 = getConn(url);
		} while (conn1 == null);

		System.out.println("refresh conn," + new Date());
		try {
			stmt1 = conn1.prepareStatement(" SELECT   c1,c2,c7 FROM t0_base order by random();");
			stmt2 = conn1.prepareStatement("SELECT /*+indexscan(t0 t0_idx2)*/ c1,c2,c8 FROM t0 where c1=? and c2=? ");
			stmt = conn1.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		try {
			stmt.executeQuery("select 1");
		} catch (Exception e) {
			return false;
		}

		return true;
	}

	public void run() {
		long c7;
		String c1, c2;
		String v_c1, v_c2, v_c8;
		int count = 0;
		long startTime = 0;
		while (!initDB())
			;
		for (int i = 0; i < 1000000000; i++) {
			try {
				ResultSet rs = stmt1.executeQuery();
				while (rs.next()) {
					try {
						if (count == 0)
							startTime = System.currentTimeMillis();
						c1 = rs.getString(1);
						c2 = rs.getString(2);
						c7 = rs.getLong(3);

						stmt2.setString(1, c1);
						stmt2.setString(2, c2);
						ResultSet rs2 = stmt2.executeQuery();

						while (rs2.next()) {
							v_c1 = rs2.getString(1);
							v_c2 = rs2.getString(2);
							v_c8 = rs2.getString(3);

							if (!v_c1.equals(c1) || !v_c2.equals(c2)) {
								// throw new Exception(String.format("bad result,par: %s,%s,ret:
								// %s,%s",c1,c2,v_c1,v_c2));
								System.out.println(String.format("bad result,par: %s,%s,%s,ret: %s,%s,%s", c1, c2,
										"" + c7, v_c1, v_c2, v_c8));
								System.exit(-1);
							}
						}
						rs2.close();
						rs2 = null;
						if (++count % 1000 == 0) {
							count = 0;
							long endTime = System.currentTimeMillis();
							System.out.println("thread id " + this.getId() + ",elipsed time=" + (endTime - startTime)
									+ "," + c1 + "," + c2);
						}
						v_c1 = null;
						v_c2 = null;
						v_c8 = null;
						c1 = null;
						c2 = null;
					} catch (Exception e) {
						if (e.getMessage().indexOf("canceling statement due") < 0)
							e.printStackTrace();
					}
				}
				rs.close();
			} catch (Exception e1) {
				if (e1.getMessage().indexOf("canceling statement due") < 0)
					e1.printStackTrace();

				try {
					stmt.executeQuery("select 1");
				} catch (Exception ee) {
					while (!initDB())
						;
				}

			}
		}

		closeConn(conn1);
	}

	public static void closeConn(Connection con) {
		try {
			con.close();
		} catch (SQLException e) {
			System.out.println("failed to close connection: " + e.getMessage());
		}
	}

	public static Connection getConn(String url) {
		try {
			if (dbType.equalsIgnoreCase("oracle"))
				Class.forName("oracle.jdbc.driver.OracleDriver");
			else if (dbType.equalsIgnoreCase("postgres"))
				Class.forName("org.postgresql.Driver");
			else if (dbType.equalsIgnoreCase("gaussdb"))
				Class.forName("com.huawei.opengauss.jdbc.Driver");
			else
				Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("error, can not find driver");
			e.printStackTrace();
			System.exit(-1);
			return null;
		}

		Connection con = null;
		try {
			con = DriverManager.getConnection(url, userName, password);
		} catch (SQLException e) {
			System.out.println("error, connect failed: " + url);
			e.printStackTrace();
			return null;
		}

		return con;
	}
}
