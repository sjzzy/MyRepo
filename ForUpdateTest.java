
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/*
 * for update并发操作，出现重复数据
测试ustore、astore
结合分区表
--ustore
 CREATE TABLE u1 (                                                                                           
     a integer,                                                                                              
     b integer,                                                                                              
     c integer GENERATED ALWAYS AS (a) STORED,                                                               
     d text GENERATED ALWAYS AS (b) STORED                                                                   
 )                                                                                                           
 WITH (orientation=row, compression=no, storage_type=ustore);                                                
 CREATE UNIQUE INDEX u1__idx36204 ON u1 USING ubtree (c, a) WITH (storage_type=USTORE) TABLESPACE pg_default;
 ALTER TABLE u1 ADD CONSTRAINT u1_a_key1 UNIQUE (a) WITH (storage_type=USTORE);                              
 CREATE INDEX u1_c_idx ON u1 USING ubtree (c) WITH (storage_type=USTORE) TABLESPACE pg_default;


--ustore partition
 CREATE TABLE u1 (                                                                                           
     a integer,                                                                                              
     b integer,                                                                                              
     c integer GENERATED ALWAYS AS (a) STORED,                                                               
     d text GENERATED ALWAYS AS (b) STORED                                                                   
 )                                                                                                           
 WITH (orientation=row, compression=no, storage_type=ustore)
 partition by range(b)
(partition test_p
start (-11000)
   end( 11000)
 every(10));                                                
 CREATE UNIQUE INDEX u1__idx36204 ON u1 USING ubtree (c, a) WITH (storage_type=USTORE) TABLESPACE pg_default;
 ALTER TABLE u1 ADD CONSTRAINT u1_a_key1 UNIQUE (a) WITH (storage_type=USTORE);                              
 CREATE INDEX u1_c_idx ON u1 USING ubtree (c) WITH (storage_type=USTORE) TABLESPACE pg_default;

--astore
drop table u1;
 CREATE TABLE u1 (                                                                                           
     a integer,                                                                                              
     b integer,                                                                                              
     c integer GENERATED ALWAYS AS (a) STORED,                                                               
     d text GENERATED ALWAYS AS (b) STORED                                                                   
 )                                                                                                           
 WITH (orientation=row, compression=no, storage_type=astore);                                                
 CREATE UNIQUE INDEX u1__idx36204 ON u1  (c, a) WITH (storage_type=aSTORE) TABLESPACE pg_default;
 ALTER TABLE u1 ADD CONSTRAINT u1_a_key1 UNIQUE (a) WITH (storage_type=aSTORE);                              
 CREATE INDEX u1_c_idx ON u1  (c) WITH (storage_type=aSTORE) TABLESPACE pg_default;
 
 
 --change schema templete
start transaction;
alter table u1 set schema tmp;
savepoint s1;
--xx
insert into u1 select a,b from tmp.u1;
end;

 */
public class ForUpdateTest extends Thread {
	private static String url = "jdbc:postgresql://10.44.133.56:5432/pg";
	private static String userName = "bank";
	private static String password = "gs_123456";
	private static String dbType = "postgres";
	public static AtomicLong count2 = new AtomicLong(0);
	public static long randomPlanSeed = 0;
	static final String version = "version: 1.4 2024-1-30";
	private static boolean standbyRead = true;
	private static int scanPCT = 100;
	static final int maxA = 1000000;

	Connection conn = null;
	PreparedStatement select = null;
	PreparedStatement select_v1 = null;
	PreparedStatement select_v2 = null;
	PreparedStatement select_v3 = null;
	PreparedStatement select_v4 = null;
	PreparedStatement insert = null;
	PreparedStatement upsert = null;
	PreparedStatement updatePK = null;
	PreparedStatement updatePK_v1 = null;
	PreparedStatement updatePK_v2 = null;
	PreparedStatement findExistedPK = null;
	PreparedStatement findNonExistedPK = null;
	PreparedStatement update = null;
	PreparedStatement update_v1 = null;
	PreparedStatement update_v2 = null;
	PreparedStatement delete = null;
	PreparedStatement delete_v1 = null;
	PreparedStatement delete_v2 = null;
	Statement checkStmt = null;
	private int index = 0;
	private static int parallel = 0;
	Random rand = new Random();
	Statement stmt = null;

	public ForUpdateTest(String svt, int id) {
		index = id;
		if (svt == null) {
			System.out.println("invalid parameter");
			return;
		}

		if (svt.equalsIgnoreCase("gaussdb")) {
			// url =
			// "jdbc:opengauss://10.145.255.132:30100,10.145.255.134:30100,10.145.255.136:30100/test?targetServerType=master&prepareThreshold=5&batchMode=on&fetchsize=2&loggerLevel=off&loggerFile=./gsjdbc.log";
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
		if (args.length != 7) {
			System.out.println(version);
			System.out.println("usage: url userName password parallel randPlanSeed(default is 0) standbyRead scanPCT");
			return;
		}
		url = args[0];
		userName = args[1];
		password = args[2];
		try {
			parallel = Integer.parseInt(args[3]);
		} catch (Exception e) {
			parallel = 100;
		}

		try {
			randomPlanSeed = Long.parseLong(args[4]);
		} catch (Exception e) {
			randomPlanSeed = 0;
		}

		standbyRead = Boolean.parseBoolean(args[5]);
		try {
			scanPCT = Integer.parseInt(args[6]);
		} catch (Exception e) {
			scanPCT = 100;
		}

		try {
			for (int i = 0; i < parallel; i++) {
				new ForUpdateTest("gaussdb", i).start();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		new ForUpdateTest("gaussdb", -1).monitor();
	}

	private boolean initDB() {
		do {
			conn = getConn(url);
		} while (conn == null);

		System.out.println("refresh conn," + new Date());
		try {
			select_v1 = conn.prepareStatement(" SELECT * from u1 where a=? for update");
			select_v2 = conn.prepareStatement(" SELECT * from u1 where c=? for update");
			select_v3 = conn.prepareStatement(" SELECT 1 from u1 where a=? for update");
			select_v4 = conn.prepareStatement(" SELECT 1 from u1 where c=? for update");
			insert = conn.prepareStatement("insert into u1(a,b) values(?,?)");
			// upsert = conn.prepareStatement("insert into u1(a,b) values(?,?) on duplicate
			// key update b=b+values(b)");
			upsert = conn.prepareStatement("insert into u1(a,b) values(?,?) on duplicate key update b=b+excluded.b");
			update_v1 = conn.prepareStatement("update u1 set b=? where a=?");
			update_v2 = conn.prepareStatement("update u1 set b=? where c=?");
			delete_v1 = conn.prepareStatement("delete from u1 where a=?");
			delete_v2 = conn.prepareStatement("delete from u1 where c=?");
			updatePK_v1 = conn.prepareStatement("update u1 set a=?, b=? where a=? ");
			updatePK_v2 = conn.prepareStatement("update u1 set a=?, b=? where c=? ");
			findExistedPK = conn.prepareStatement("select a from u1 where a in (select generate_series(1," + (maxA - 1)
					+ ") order by random() )  limit 100");
			findNonExistedPK = conn.prepareStatement("select * from (select c1 from (select * from generate_series(1,"
					+ (maxA - 1)
					+ ") c1 order by random())  left join u1 on c1=a where a is null limit 10000) order by random() limit 100");
			checkStmt = conn.createStatement();

			stmt = conn.createStatement();
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

	private void forUpdate() {
		while (true) {
			try {
				int id = genId();
				switch (rand.nextInt(4)) {
				case 0:
					select = select_v1;
					break;
				case 1:
					select = select_v2;
					break;
				case 2:
					select = select_v3;
					break;
				default:
					select = select_v4;
					break;
				}
				select.setInt(1, id);
				ResultSet rs = select.executeQuery();
				if (rs.next()) {
					if (rand.nextBoolean())
						update = update_v1;
					else
						update = update_v2;
					update.setInt(1, genB());
					update.setInt(2, id);
					int ret = update.executeUpdate();
					if (ret != 1) {
						System.out.println("bad result,ret=" + ret + "," + update.toString());
						System.exit(-1);
					}
				} else {
					insert.setInt(1, id);
					insert.setInt(2, genB());
					int ret = insert.executeUpdate();
					if (ret != 1) {
						System.out.println("bad result,ret=" + ret + "," + insert.toString());
						System.exit(-1);
					}
				}
				rs.close();
				rs = null;
				if (rand.nextInt(100) < 70)
					conn.commit();
				else
					conn.rollback();

				count2.getAndIncrement();
			} catch (Exception e1) {
				if (e1.getMessage().indexOf("canceling statement due") < 0
						&& e1.getMessage().indexOf("duplicate key value violates unique") < 0) {
					System.out.println(e1.getMessage());
					// e1.printStackTrace();
				}

				try {
					conn.rollback();
				} catch (Exception ee) {
				}

				try {
					stmt.executeQuery("select 1");
				} catch (Exception ee) {
					while (!initDB())
						;
				}
			}
		}
	}

	private void check() {
		int sqlNum = 14;
		String[] sql = new String[sqlNum];
		int id = 0;

		// DTS2024020204675
		// Index Only Scan using u1__idx36204, target is a
		sql[0] = "select /*+ no tablescan(u1) */ a,1 from u1 where a between %d and %d and c between %d and %d group by 1,2 having count(*) >1";
		// using different indexes
		sql[1] = "select /*+indexonlyscan(u1 u1__idx36204) */ a,c from u1 where a between %d and %d  minus all select  /*+indexscan(u1 u1_c_idx) */  a,c from u1 where c between %d and %d";
		sql[2] = "select /*+indexscan(u1 u1_c_idx) */ a,c from u1 where c between %d and %d  minus all select  /*+indexonlyscan(u1 u1__idx36204) */ a,c from u1 where a between %d and %d";

		// using different indexes
		sql[3] = "select /*+indexscan(u1 u1_a_key1) */ a,c from u1 where a between %d and %d minus all select /*+indexscan(u1 u1_c_idx) */ a,c from u1 where c between %d and %d";
		sql[4] = "select /*+indexscan(u1 u1_c_idx) */ a,c from u1 where c between %d and %d minus all select  /*+indexscan(u1 u1_a_key1) */ a,c from u1 where a between %d and %d";

		// Index Only Scan using u1__idx36204, target is a
		sql[5] = "select /*+ no tablescan(u1) */ a,1 from u1 where a between %d and %d and %d <= %d group by 1,2 having count(*) >1";

		// Index Only Scan using u1__idx36204, target is c
		sql[6] = "select /*+ no tablescan(u1) */ c,1 from u1 where a between %d and %d and c between %d and %d group by 1,2 having count(*) >1";
		// BitmapOr, target is a
		sql[7] = "select /*+ no tablescan(u1)  no indexonlyscan(u1 u1__idx36204) no indexscan(u1 u1_a_key1) */ a,1 from u1 where a between %d and %d or c between %d and %d group by 1,2 having count(*) >1";
		// BitmapOr, target is c
		sql[8] = "select /*+ no tablescan(u1)  no indexonlyscan(u1 u1__idx36204) no indexscan(u1 u1_c_idx) */ c,1 from u1 where a between %d and %d or c between %d and %d group by 1,2 having count(*) >1";

		// seqscan takes a lot of time due to DTS2024020117713
		sql[9] = "select a,1 from u1 where %d <= %d and %d <= %d group by 1,2 having count(*) >1";
		sql[10] = "select c,1 from u1 where %d <= %d and %d <= %d group by 1,2 having count(*) >1";
		sql[11] = "select a,c from u1 where a!=c";

		sql[12] = "select /*+tablescan(u1)     */ a,c from u1 where %d <= %d  minus all select  /*+no tablescan(u1) */ a,c from u1 where  %d  <= %d"; // DTS2024010908103
		sql[13] = "select /*+no tablescan(u1)  */ a,c from u1 where %d <= %d  minus all select  /*+   tablescan(u1) */ a,c from u1 where  %d  <= %d";
		if (scanPCT < 10000) {
			sqlNum = 9;
		}

		try {
			checkStmt.execute("set plan_mode_seed=" + randomPlanSeed);
		} catch (Exception e1) {
		}
		int index = 0;
		String execSQL = null;
		while (true) {
			try {
				index = rand.nextInt(sqlNum);
				id = genId(maxA - scanPCT / 10000 * maxA);
				execSQL = String.format(sql[index], id, (int) (id + scanPCT / 10000.0 * maxA), id,
						(int) (id + scanPCT / 10000.0 * maxA));
				ResultSet rs = checkStmt.executeQuery(execSQL);
				if (rs.next()) {
					System.out.println("bad result,rs=" + rs.getInt(1) + "," + rs.getInt(2) + ":" + sql[index]);
					while (rs.next()) {
						System.out.println("bad result,rs=" + rs.getInt(1) + "," + rs.getInt(2));
					}
					rs.close();
					rs = checkStmt.executeQuery("select plan_seed();");
					if (rs.next()) {
						System.out.println("plan seed is " + rs.getLong(1));
					}
					System.exit(-1);
				}
				rs.close();
				rs = null;
				if (rand.nextInt(100) < 10)
					conn.commit();
				count2.getAndIncrement();
			} catch (Exception e1) {
				String msg = e1.getMessage();
				if (msg != null) {
					System.out.println(e1.getMessage());
					System.out.println(execSQL);
				} else {
					e1.printStackTrace();
					System.out.println(execSQL);
				}

				try {
					conn.rollback();
				} catch (Exception ee) {
				}

				try {
					stmt.executeQuery("select 1");
				} catch (Exception ee) {
					while (!initDB())
						;
					try {
						checkStmt.execute("set plan_mode_seed=" + randomPlanSeed);
					} catch (Exception e) {

					}
				}
			}
		}
	}

	public void monitor() {
		long runTime = 3600 * 24 * 30;
		long endTime = System.currentTimeMillis() + runTime * 1000L;
		long pre = count2.get();
		long curr = 0L;
		while (System.currentTimeMillis() <= endTime) {
			try {
				Thread.sleep(3000L);
			} catch (InterruptedException localInterruptedException) {
			}
			curr = count2.get();
			System.out.println("tps = " + (curr - pre) / 3L + ",total = " + curr + ", " + new Date());
			pre = curr;
		}
		System.out.println("test finished: total = " + curr + ", " + System.currentTimeMillis());
	}

	private void delete() {
		while (true) {
			try {
				int id = genId();
				if (rand.nextBoolean())
					delete = delete_v1;
				else
					delete = delete_v2;
				delete.setInt(1, id);
				int ret = delete.executeUpdate();
				if (ret > 1) {
					System.out.println("bad result,ret=" + ret + "," + delete.toString());
					System.exit(-1);
				}

				// ResultSet rs=stmt.executeQuery("select plan_seed();");
				// if(rs.next())
				// System.out.println("delete: plan seed is " + rs.getLong(1));

				if (rand.nextInt(100) < 60)
					conn.commit();
				else if (rand.nextInt(100) < 10)
					conn.rollback();
				count2.getAndIncrement();
			} catch (Exception e1) {
				if (e1.getMessage().indexOf("canceling statement due") < 0) {
					System.out.println(e1.getMessage());
					// e1.printStackTrace();
				}

				try {
					conn.rollback();
				} catch (Exception ee) {
				}

				try {
					stmt.executeQuery("select 1");
				} catch (Exception ee) {
					while (!initDB())
						;
				}
			}
		}
	}

	private void upsert() {
		try {
			int id = genId();
			upsert.setInt(1, id);
			upsert.setInt(2, genB());
			int ret = upsert.executeUpdate();
			if (ret != 1) {
				System.out.println("bad result,ret=" + ret + "," + upsert.toString());
				System.exit(-1);
			}

			if (rand.nextInt(100) < 60)
				conn.commit();
			else if (rand.nextInt(100) < 10)
				conn.rollback();
			count2.getAndIncrement();
		} catch (Exception e1) {
			if (e1.getMessage().indexOf("canceling statement due") < 0)
			{
				System.out.println(e1.getMessage());
				//e1.printStackTrace();
			}
			try {
				conn.rollback();
			} catch (Exception ee) {
			}

			try {
				stmt.executeQuery("select 1");
			} catch (Exception ee) {
				while (!initDB())
					;
			}
		}

	}

	private void updatePK() {
		try {
			ResultSet rs1 = findExistedPK.executeQuery();
			ResultSet rs2 = findNonExistedPK.executeQuery();
			int a1, a2;
			stmt.execute("savepoint s1;");
			while (rs1.next() && rs2.next()) {
				a1 = rs1.getInt(1);
				a2 = rs2.getInt(1);
				if (rand.nextBoolean())
					updatePK = updatePK_v1;
				else
					updatePK = updatePK_v2;

				try {
					updatePK.setInt(1, a1);
					updatePK.setInt(2, genB());
					updatePK.setInt(3, a2);
					int ret = updatePK.executeUpdate();
					if (ret > 0 || rand.nextInt(100) < 30)
						break;
				} catch (SQLException e2) {
					System.out.println(e2.getMessage());
					stmt.execute("rollback to s1");
				}
			}
			rs1.close();
			rs1 = null;
			rs2.close();
			rs2 = null;
			if (rand.nextInt(100) < 70)
				conn.commit();
			else
				conn.rollback();
			count2.getAndIncrement();
		} catch (Exception e1) {
			if (e1.getMessage().indexOf("canceling statement due") < 0)
			{
				System.out.println(e1.getMessage());
				//e1.printStackTrace();
			}
			try {
				conn.rollback();
			} catch (Exception ee) {
			}

			try {
				stmt.executeQuery("select 1");
			} catch (Exception ee) {
				while (!initDB())
					;
			}
		}

	}

	private int genId() {
		return rand.nextInt(maxA);
	}

	private int genId(int max) {
		if(max<=0)
			return 0;
		return rand.nextInt(max);
	}

	private int genB() {
		return rand.nextInt(11000);
	}

	public void run() {
		while (!initDB())
			;

		if (standbyRead) {
			check();
			return;
		}

		if (index == 0)
			check();
		else if (index * 100.0 / parallel < 50)
			forUpdate();
		else if (index * 100.0 / parallel < 70)
			updatePK();
		else if (index * 100.0 / parallel < 85)
			upsert();
		else
			delete();

		closeConn(conn);
	}

	public static void closeConn(Connection con) {
		try {
			con.close();
		} catch (SQLException e) {
			System.out.println("failed to close connection: " + e.getMessage());
		}
	}

	public Connection getConn(String url) {
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
			con.setAutoCommit(false);
		} catch (SQLException e) {
			System.out.println("error, connect failed: " + url);
			e.printStackTrace();
			return null;
		}

		return con;
	}
}
