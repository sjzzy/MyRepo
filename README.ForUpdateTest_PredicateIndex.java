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
 drop table u1_pred;
  create table u1_pred (a int,b int,c int GENERATED ALWAYS AS (a) stored,d text GENERATED ALWAYS AS (b) );
  create index u1_pred_idx on u1_pred(c);
create index u1_pred_idx_ac on u1_pred(c,a);


create index concurrently u1_pred_a1 on u1_pred(a) where b<=1000;
create index concurrently u1_pred_a2 on u1_pred(a) where b between 1000 and 2000;
create index concurrently u1_pred_a3 on u1_pred(a) where b between 2000 and 3000;
create index concurrently u1_pred_a4 on u1_pred(a) where b between 3000 and 4000;
create index concurrently u1_pred_a5 on u1_pred(a) where b between 4000 and 5000;
create index concurrently u1_pred_a6 on u1_pred(a) where b between 5000 and 6000;
create index concurrently u1_pred_a7 on u1_pred(a) where b between 6000 and 7000;
create index concurrently u1_pred_a8 on u1_pred(a) where b between 7000 and 8000;
create index concurrently u1_pred_a9 on u1_pred(a) where b between 8000 and 9000;
create index concurrently u1_pred_a10 on u1_pred(a) where b between 9000 and 10000;
create index concurrently u1_pred_a11 on u1_pred(a) where b between 10000 and 11000;

create  index concurrently u1_pred_c1 on  u1_pred(c) where b<=1000+500;
create  index concurrently u1_pred_c2 on  u1_pred(c) where b between 1000  and 500+ 2000;
create  index concurrently u1_pred_c3 on  u1_pred(c) where b between 2000  and 500+ 3000;
create  index concurrently u1_pred_c4 on  u1_pred(c) where b between 3000  and 500+ 4000;
create  index concurrently u1_pred_c5 on  u1_pred(c) where b between 4000  and 500+ 5000;
create  index concurrently u1_pred_c6 on  u1_pred(c) where b between 5000  and 500+ 6000;
create  index concurrently u1_pred_c7 on  u1_pred(c) where b between 6000  and 500+ 7000;
create  index concurrently u1_pred_c8 on  u1_pred(c) where b between 7000  and 500+ 8000;
create  index concurrently u1_pred_c9 on  u1_pred(c) where b between 8000  and 500+ 9000;
create  index concurrently u1_pred_c10 on u1_pred(c) where b between 9000  and 500+ 10000;
create  index concurrently u1_pred_c11 on u1_pred(c) where b between 10000 and 500+ 11000;
 */
public class ForUpdateTest_PredicateIndex extends Thread {
	private static String url = "jdbc:postgresql://10.44.133.56:5432/pg";
	private static String userName = "bank";
	private static String password = "gs_123456";
	private static String dbType = "postgres";
	public static AtomicLong count2 = new AtomicLong(0);
	public static long randomPlanSeed=0;
	static final String version="version: 1.0 2023-12-20"; 
	private static boolean standbyRead = true;

	Connection conn = null;
	PreparedStatement select = null;
	PreparedStatement select_v1 = null;
	PreparedStatement select_v2 = null;
	PreparedStatement select_v3 = null;
	PreparedStatement select_v4 = null;
	PreparedStatement insert = null;
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

	public ForUpdateTest_PredicateIndex(String svt, int id) {
		index = id;
		if (svt == null) {
			System.out.println("invalid parameter");
			return;
		}

		if (svt.equalsIgnoreCase("gaussdb")) {
			//url = "jdbc:opengauss://10.145.255.132:30100,10.145.255.134:30100,10.145.255.136:30100/test?targetServerType=master&prepareThreshold=5&batchMode=on&fetchsize=2&loggerLevel=off&loggerFile=./gsjdbc.log";
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
		final String USAGE="url userName password parallel randPlanSeed(default is 0) standby_read";
		if (args.length != 6) {
			System.out.println(version);
			System.out.println("usage: " + USAGE);			
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
		
		standbyRead=Boolean.parseBoolean(args[5]);

		try {
			for (int i = 0; i < parallel; i++) {
				new ForUpdateTest_PredicateIndex("gaussdb", i).start();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		new ForUpdateTest_PredicateIndex("gaussdb", -1).monitor();
	}

	private boolean initDB() {
		do {
			conn = getConn(url);
		} while (conn == null);

		System.out.println("refresh conn," + new Date());
		try {
			select_v1 = conn.prepareStatement(" SELECT * from u1_pred where a=? and b=? for update");
			select_v2 = conn.prepareStatement(" SELECT * from u1_pred where c=? and b=? for update");
			select_v3 = conn.prepareStatement(" SELECT 1 from u1_pred where a=? and b=? for update");
			select_v4 = conn.prepareStatement(" SELECT 1 from u1_pred where c=? and b=? for update");
			insert = conn.prepareStatement("insert into u1_pred(a,b) values(?,?)");
			update_v1 = conn.prepareStatement("update u1_pred set b=? where a=? and b=?");
			update_v2 = conn.prepareStatement("update u1_pred set b=? where c=? and b=?");
			delete_v1 = conn.prepareStatement("delete from u1_pred where a=? and b=?");
			delete_v2 = conn.prepareStatement("delete from u1_pred where c=? and b=?");
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
				int b = genB();
				switch(rand.nextInt(4))
				{
				case 0:
					select=select_v1;
					break;
				case 1:
					select=select_v2;
					break;
				case 2: 
					select=select_v3;
					break;
				default:
					select=select_v4;
					break;
				}	
				select.setInt(1, id);
				select.setInt(2, b);
				ResultSet rs = select.executeQuery();
				if (rs.next()) {
					if(rand.nextBoolean())
						update=update_v1;
					else
						update=update_v2;
					update.setInt(1, genB());
					update.setInt(2, id);
					update.setInt(3, b);
					int ret = update.executeUpdate();
					if (ret < 1) { // not unique, so only check <1
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
				if(rand.nextInt(100)< 97)
					conn.commit();
				else
					conn.rollback();
				count2.getAndIncrement();
			} catch (Exception e1) {
				if (e1.getMessage().indexOf("canceling statement due") < 0
						&& e1.getMessage().indexOf("duplicate key value violates unique") < 0)
					e1.printStackTrace();

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
		String []sql=new String[2];
		//sql[0]="select a,b from u1_pred group by 1,2 having count(*) >1";
		sql[1]="select /*+tablescan(u1_pred)  */ a,c from u1_pred minus all select  /*+no tablescan(u1_pred) */ a,c from u1_pred ";
		sql[0]="select /*+ no tablescan(u1_pred) */ a,c from u1_pred minus all select  /*+tablescan(u1_pred) */ a,c from u1_pred ";
		

		try {
			checkStmt.execute("set plan_mode_seed="+ randomPlanSeed);
		}catch(Exception e1) {
			
		}
		String execSQL;
		long startTime=0;
		while (true) {
			try {
				if(rand.nextInt(10)<5) {
					int index=rand.nextInt(11);
					if(rand.nextInt(10)<5) {
						execSQL=String.format("select /*+tablescan(u1_pred)  */ a,-1 from u1_pred where b between %d and %d "
								+ "minus all select  /*+no tablescan(u1_pred) */ a,-1 from u1_pred where b between %d and %d "
								,index*1000,index*1000+1000,index*1000,index*1000+1000);
					}else {
						execSQL=String.format("select /*+no tablescan(u1_pred)  */ a,-2 from u1_pred where b between %d and %d "
								+ "minus all select  /*+tablescan(u1_pred) */ a,-2 from u1_pred where b between %d and %d "
								,index*1000,index*1000+1000,index*1000,index*1000+1000);
					}
				} else if(rand.nextInt(10)<9) {
					int index=rand.nextInt(11);
					if(rand.nextInt(10)<5) {
						execSQL=String.format("select /*+tablescan(u1_pred)  */ c,-3 from u1_pred where b between %d and %d "
								+ "minus all select  /*+no tablescan(u1_pred) */ c,-3 from u1_pred where b between %d and %d "
								,index*1000+500,index*1000+1500,index*1000+500,index*1000+1500);
					}else {
						execSQL=String.format("select /*+no tablescan(u1_pred)  */ c,-4 from u1_pred where b between %d and %d "
								+ "minus all select  /*+tablescan(u1_pred) */ c,-4 from u1_pred where b between %d and %d "
								,index*1000+500,index*1000+1500,index*1000+500,index*1000+1500);
					}
				}
				else {
					int index =rand.nextInt(sql.length);
					execSQL=sql[index];
				}
				startTime=System.currentTimeMillis();
				ResultSet rs = checkStmt.executeQuery(execSQL);
				
				if (rs.next()) {
					System.out.println("bad result:" + rs.getInt(2) + ":" + execSQL);
					System.out.println("rs=" + rs.getInt(1) + "," + rs.getInt(2) );
					while (rs.next()) {
						System.out.println("rs=" + rs.getInt(1) + "," + rs.getInt(2) );
					}
					rs.close();
					rs=checkStmt.executeQuery("select plan_seed();");
					if(rs.next()) {
						System.out.println("plan seed is " + rs.getLong(1));
					}
					System.exit(-1);
				}
				rs.close();
				rs = null;
				if (rand.nextInt(100) < 50)
					conn.commit();
				count2.getAndIncrement();
			} catch (Exception e1) {
				if (standbyRead || e1.getMessage().indexOf("canceling statement due") < 0) {
					System.out.println(new Date().toString() + ", time=" + (System.currentTimeMillis() - startTime)/1000 + "s," + e1.getMessage());					
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
						checkStmt.execute("set plan_mode_seed="+ randomPlanSeed);
					}catch(Exception e) {
						
					}				
				}
			}
		}
	}
	public void monitor()
	  {
		long runTime=3600*24*30;
	    long endTime = System.currentTimeMillis() + runTime * 1000L;
	    long pre = count2.get();
	    long curr = 0L;
	    while (System.currentTimeMillis() <= endTime) {
	      try {
	        Thread.sleep(3000L);
	      }
	      catch (InterruptedException localInterruptedException) {
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
				int b=genB();
						
				if(rand.nextBoolean())
					delete=delete_v1;
				else
					delete=delete_v2;
				delete.setInt(1, id);
				delete.setInt(2, b);
				int ret = delete.executeUpdate();
				//not unqiue, so do not check
//				if (ret > 1) {
//					System.out.println("bad result,ret=" + ret + "," + delete.toString());
//					System.exit(-1);
//				}
				
//				ResultSet rs=stmt.executeQuery("select plan_seed();");
//				if(rs.next())
//					System.out.println("delete: plan seed is " + rs.getLong(1));
				
				if(rand.nextInt(100)< 50)
					conn.commit();
				count2.getAndIncrement();
			} catch (Exception e1) {
				if (e1.getMessage().indexOf("canceling statement due") < 0)
					e1.printStackTrace();
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

	private int genId() {
		return rand.nextInt(1000000);
	}
	
	private int genB() {
		return rand.nextInt(11000);
	}

	public void run() {
		while (!initDB())
			;
		
		if(standbyRead) {
			check();
			return;
		}
		
		if (index == 0)
			check();
		else if (index * 100.0 / parallel < 80)
			forUpdate();
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
