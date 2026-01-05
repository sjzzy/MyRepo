

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
//gaussdb jdbc:gaussdb://10.185.180.166:29900,10.185.180.168:29900,10.185.180.172:29900/tpcc?targetServerType=master&prepareThreshold=5&batchMode=on&fetchsize=2&loggerLevel=off&loggerFile=./gsjdbc.log 10 test gs_123456 1000000 30 false
//oracle jdbc:oracle:thin:@10.90.58.120:1521:cdb1 10 test gs_123456 1000000 30 false

/*
create table epq_t1 (pk serial primary key , id int, value int8, 
    c1 varchar(1000) GENERATED ALWAYS AS (((value)::text || 'AAAAAAAAAAAAAAAAAAAAAAAA'::text)) STORED);
create index epq_t1_idx on epq_t1(id);

create table epq_t2 (pk serial primary key, id int, value int8);
create index epq_t2_idx on epq_t2(id);

--每个id，有10个重复值, 对应t2Size
insert into epq_t2(id, value) select id, v from (select rownum id ,0 v from pg_class, pg_class b where rownum<=10000), (select * from pg_class limit 10) v;
--每个id，有1000个重复值, 对应t1Size
insert into epq_t1(id, value) select id, value from (select * from pg_class limit 100),epq_t2;


check:
select  'bad1', id, value from epq_t1 minus select  'bad1', id, value::varchar from epq_t2;
select  'bad2', id, value, count(*)  from epq_t1 group by 1,2,3 having count(*)!=1000;


 */

public class EPQ extends Thread {
	private int id = 0;
	
	private Connection conn = null;
	private Random r = new Random();
	private Statement checkStmt = null;
	private Statement execStmt = null;

	private PreparedStatement upsertStmt = null;
	private PreparedStatement updateT1Stmt = null;
	private PreparedStatement updateT1Stmt_seqscan = null;

	private PreparedStatement updateT2Stmt = null;

	private PreparedStatement selectPK = null;
 	private PreparedStatement upsertT1 = null;
 	
	private PreparedStatement selectTwoTables = null;


	private volatile OP op ;


	private static int parallel = 1000;
	private static String dbType = "postgres";
	private static String url = null;
	private static String username = null;
	private static String password = null;
	private static final int t1Size=1000;
	private static final int t2Size=10;
	
	private static int batchSize=t2Size * t1Size;
	
	private static int maxId=10000;
	private static boolean useGenerateColumn=true;
	private static String runMode="select";
	private static int commitPct=70;


	private static String checkSQL = null;
	
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static AtomicLong count2 = new AtomicLong(0L);

	public EPQ(int i) {
		//this.id;
		try {
			if (dbType.equalsIgnoreCase("oracle")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
			else if (dbType.equalsIgnoreCase("postgres"))
				Class.forName("org.postgresql.Driver");
			else if (dbType.equalsIgnoreCase("gaussdb"))
				Class.forName("com.huawei.gaussdb.jdbc.Driver");
			else
				Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		this.op=OP.UPDATE;
	}
	
	public void setOP(OP o) {
		this.op=o;
	}
	
	public boolean initDB() {
		while(true) {
			this.conn = getConn(url, username, password);
			assert(conn != null);
			try {
				updateT1Stmt = conn.prepareStatement("update /*+ no tablescan(epq_t1) */  epq_t1 set value=value + ? where id=?");
				updateT2Stmt = conn.prepareStatement("update epq_t2 set value=value + ? where id=?");
				updateT1Stmt_seqscan = conn.prepareStatement("update /*+ tablescan(epq_t1) set(query_dop 1) */  epq_t1 set value=value + ? where id=?");
				
				selectPK=conn.prepareStatement("select pk from epq_t1 where id=?");
				//upsertT1=conn.prepareStatement("insert into epq_t1 select pk,id,? from epq_t1 where id=? fro update on duplicate key update value = value + excluded.value");
				upsertT1=conn.prepareStatement("insert into epq_t1 values(?,?,?) on duplicate key update value = value + excluded.value,id=excluded.id");

				
				selectTwoTables = conn.prepareStatement("select t1.id, t2.id, t1.value,t2.value from epq_t1 t1, epq_t2 t2 where t1.id=t2.id and t2.id=?  for update");
				
				checkStmt =  conn.createStatement();
				execStmt = conn.createStatement();
			} catch (SQLException e) {
				e.printStackTrace();
				freeConn();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				continue;				
			}
			
			return true;
		}
	}
	
	public void doOneTask(String taskName, int times, int sleepTime) {
		long startTime = 0;
		long endTime =  0;
		startTime = System.currentTimeMillis();
		
		if(taskName.equalsIgnoreCase("update"))
			update();
		else
			System.out.println(taskName + " not supported, skipped");
		
		endTime = System.currentTimeMillis();
		if(sleepTime>0) {
			try {
				Thread.sleep(sleepTime*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void run() {
		initDB();
		
		if(op == OP.MONITOR) {
			monitor();
			return;
		}
		
		if(op == OP.UPDATE)
			update();
		else if(op == OP.UPDATE2)
			update2();
		else if(op == OP.UPDATE3)
			update3();
		else if(op == OP.FOR_UPDATE_TWO_TABLES)
			for_update_two_tables();

	}
	
	private int genId() {
		return r.nextInt(maxId) + 1;
	}
	private void refreshConnIfNecessary() {
		if(conn!=null) {
			try {
				checkStmt.executeQuery(checkSQL);
				return ;
			}catch(SQLException se) {
				System.out.println("check stmt," + se.getMessage());
				freeConn();
			}
		}
		initDB();
	}
	
	private void refreshConn() {
		freeConn();
		initDB();
	}
	
	public void update() {
		for (int i = 1; i <= 100000000; i++) {
			try {
				long delta=r.nextInt() - 1073741824 ; //power(2,30);
				id=genId();
				updateT1Stmt.setLong(1, delta);
				updateT1Stmt.setLong(2, id); 
				if (updateT1Stmt.executeUpdate() != t1Size) {
					System.err.println("bad update , stmt:" + updateT1Stmt.toString());
					System.exit(-1);
				}
				
				updateT2Stmt.setLong(1, delta);
				updateT2Stmt.setLong(2, id); 
				if (updateT2Stmt.executeUpdate() != t2Size) {
					System.err.println("bad update , stmt:" + updateT2Stmt.toString());
					System.exit(-1);
				}
				
				if (this.r.nextInt(100) < commitPct)
					this.conn.commit();
				else
					this.conn.rollback();
				count2.incrementAndGet();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					this.conn.rollback();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
				refreshConnIfNecessary();
			}
		}
	}

	
	public void update2() {
		for (int i = 1; i <= 100000000; i++) {
			try {
				long delta=r.nextInt() - 1073741824 ; //power(2,30);
				id=genId();
				updateT1Stmt_seqscan.setLong(1, delta);
				updateT1Stmt_seqscan.setLong(2, id); 
				if (updateT1Stmt_seqscan.executeUpdate() != t1Size) {
					System.err.println("bad update , stmt:" + updateT1Stmt_seqscan.toString());
					System.exit(-1);
				}
				
				updateT2Stmt.setLong(1, delta);
				updateT2Stmt.setLong(2, id); 
				if (updateT2Stmt.executeUpdate() != t2Size) {
					System.err.println("bad update , stmt:" + updateT2Stmt.toString());
					System.exit(-1);
				}
				
				if (this.r.nextInt(100) < commitPct)
					this.conn.commit();
				else
					this.conn.rollback();
				count2.incrementAndGet();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					this.conn.rollback();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
				refreshConnIfNecessary();
			}
		}
	}
	
	public void update3() {
		for (int i = 1; i <= 100000000; i++) {
			try {
				long delta=r.nextInt() - 1073741824 ; //power(2,30);
				id=genId();
				int count=0;
				int pk= 0;
				selectPK.setInt(1, id);
				ResultSet rs = selectPK.executeQuery();
				while(rs.next()) {
					pk=rs.getInt(1);
					
					upsertT1.setInt(1, pk);					
					upsertT1.setInt(2, id);
					upsertT1.setLong(3, delta);
					if (upsertT1.executeUpdate() != 1) {//based on PK
						System.err.println("bad update , stmt:" + upsertT1.toString());
						System.exit(-1);
					}
					count++;
				}
				if(count!=t1Size) {
					System.err.println("bad select , stmt:" + selectPK.toString());
					System.exit(-1);
				}
				updateT2Stmt.setLong(1, delta);
				updateT2Stmt.setLong(2, id); 
				if (updateT2Stmt.executeUpdate() != t2Size) {
					System.err.println("bad update , stmt:" + updateT2Stmt.toString());
					System.exit(-1);
				}
				
				if (this.r.nextInt(100) < commitPct)
					this.conn.commit();
				else
					this.conn.rollback();
				count2.incrementAndGet();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					this.conn.rollback();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
				refreshConnIfNecessary();
			}
		}
	}
	
	/*
	 * 
	 * start transaction;
	 * select t1.id, t2.id, t1.value,t2.value from epq_t1 t1, epq_t2 t2 where t1.id=t2.id and t2.id=?  for update;
	 *  update /*+ no tablescan(epq_t1) * /  epq_t1 set value=value + ? where id=?;
	 * update epq_t2 set value=value + ? where id=?;
	 * select t1.id, t2.id, t1.value,t2.value from epq_t1 t1, epq_t2 t2 where t1.id=t2.id and t2.id=?  for update;
	 * select  *,count(*) from  (select t1.id, t2.id, t1.value,t2.value from epq_t1 t1, epq_t2 t2 where t1.id=t2.id and t2.id=? for update) group by 1,2,3,4 order by count(*) desc ;
	 * end;
	 */
	public void for_update_two_tables() {
		for (int i = 1; i <= 100000000; i++) {
			try {
				long delta=r.nextInt() - 1073741824 ; //power(2,30);
				id=genId();
				int count=0;

				selectTwoTables.setInt(1, id);
				ResultSet rs = selectTwoTables.executeQuery();
				long expectValue=0;
				while(rs.next()) {
					int t1_id=rs.getInt(1);
					int t2_id=rs.getInt(2);
					long t1_value=rs.getLong(3);
					long t2_value=rs.getLong(4);
					if(count==0)
						expectValue=t1_value;
					if(t1_id != t2_id || t1_value != t2_value || t1_id != id || expectValue!=t2_value ) {
						System.err.println("bad select , stmt:" + selectTwoTables.toString());
						System.err.println(String.format("detail expect=%d,id=%d,t1_id=%d,t2_id=%d,t1_value=%d,t2_value=%d",expectValue,id, t1_id,t2_id,t1_value,t2_value));
						System.exit(-1);
					}
					
					count++;
				}
				rs.close();
				
				if(count!=t1Size *t2Size) {
					System.err.println("bad select , stmt:" + selectTwoTables.toString());
					System.exit(-1);
				}
				updateT1Stmt.setLong(1, delta);					
				updateT1Stmt.setInt(2, id);
				if (updateT1Stmt.executeUpdate() != t1Size) {
					System.err.println("bad update , stmt:" + updateT1Stmt.toString());
					System.exit(-1);
				}
				
				updateT2Stmt.setLong(1, delta);
				updateT2Stmt.setLong(2, id); 
				if (updateT2Stmt.executeUpdate() != t2Size) {
					System.err.println("bad update , stmt:" + updateT2Stmt.toString());
					System.exit(-1);
				}
				
				//double check
				count=0;
				selectTwoTables.setInt(1, id);
				rs = selectTwoTables.executeQuery();
				while(rs.next()) {
					int t1_id=rs.getInt(1);
					int t2_id=rs.getInt(2);
					long t1_value=rs.getLong(3);
					long t2_value=rs.getLong(4);
					if(count==0)
						expectValue=t1_value;
					if(t1_id != t2_id || t1_value != t2_value || t1_id !=id || expectValue!=t2_value ) {
						System.err.println("bad select after update , stmt:" + selectTwoTables.toString());
						System.err.println(String.format("detail expect=%d,id=%d,t1_id=%d,t2_id=%d,t1_value=%d,t2_value=%d",expectValue,id, t1_id,t2_id,t1_value,t2_value));
						System.exit(-1);
					}
					
					count++;
				}
				rs.close();
								
				if (this.r.nextInt(100) < commitPct)
					this.conn.commit();
				else
					this.conn.rollback();
				count2.incrementAndGet();
			} catch (Exception e) {
				e.printStackTrace();
				try {
					this.conn.rollback();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
				refreshConnIfNecessary();
			}
		}
	}
	
	public void monitor() {
		long pre = count2.get();
		long curr = 0L;
		while (true) {
			try {
				Thread.sleep(3000L);
			} catch (InterruptedException localInterruptedException) {
			}
			curr = count2.get();
			System.out.println("tps = " + (curr - pre) / 3L + ",total = " + curr + ", " + sdf.format(new Date()));
			pre = curr;
		}
		
	}

	
	enum OP{
		UPDATE,UPDATE2,UPDATE3,FOR_UPDATE_TWO_TABLES, MONITOR
	};
	

	public static void main(String[] args) {
		String usage = "dbType url parallel username password batchSize maxId useGenerateColumn runMode commitPct";
		
		if (args.length != usage.split(" ").length) {
			System.out.println("version 1.0\nusage:" + usage);
			System.out.println("\trunMode:" + "UPDATE");
			return;
		}

		dbType = args[0];
		if(dbType.equalsIgnoreCase("oracle")) {
			checkSQL="select 1 from dual";
		}else {
			checkSQL="select 1";
		}
			
		url = args[1];
		try {
			parallel = Integer.parseInt(args[2]);
		} catch (Exception e) {
			System.out.println("error occured while parsing parameter, use default parallel:" + parallel + "\n detail:"
					+ e.getMessage());
		}

		username = args[3];
		password = args[4];
		batchSize = Integer.parseInt(args[5]);
		maxId= Integer.parseInt(args[6]);
		useGenerateColumn= Boolean.parseBoolean(args[7]);
		runMode = args[8];
		commitPct = Integer.parseInt(args[9]);

		
		EPQ monitor = new EPQ(0);
		monitor.setOP(OP.MONITOR);
		monitor.start();
			
		EPQ threads[] = new EPQ[parallel];
		for (int i = 0; i < parallel; i++) {
			threads[i]= new EPQ(i);
			if(i < 3)
				threads[i].op = OP.UPDATE2;
			else if(i < 50)
				threads[i].op = OP.UPDATE3;
			else if(i < 450)
				threads[i].op = OP.FOR_UPDATE_TWO_TABLES;
			else 
				threads[i].op = OP.UPDATE;
		}
		
		
		for (int i = 0; i < parallel; i++) {
			threads[i].start();
		}		
	}
	
	private void freeConn() {
		try {
			if (conn != null)
				conn.close();
		} catch (Exception localException) {
		}
		conn = null;
	}

	private Connection getConn(String url, String username, String password) {
		Connection localCon = null;
		while(true) {
			try {
				localCon = DriverManager.getConnection(url, username, password);
			} catch (SQLException e) {
				System.out.println("error, connect failed: " + url + "," + e.getMessage());
				localCon=null;
			}
			if(localCon != null)
				break;
			
			try {
				Thread.sleep(100);
			}catch(InterruptedException ie) {
				
			}
		}
		
		try {
			localCon.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return localCon;
	}
}
