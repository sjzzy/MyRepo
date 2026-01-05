import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentTest extends Thread{
	private static Properties properties = new Properties();
	private static final String version = "1.0.3";
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private static String dbType = null;
	private static String urls[] = null;
	private static String userName=null;
	private static String password=null;
	private static long   runTime=0;
	private static int	  numOfConn=0;
	private static boolean needStop = false;
	private static String sqlFile=null;
	private static String initSQL=null;
	private static boolean fixedSQL=false;
	private static int interval = 0;
	private static AtomicLong count2 = new AtomicLong(0);
	
	private static LinkedList<String> cmds = null;
	
	
	private Connection conn = null; 
	private Statement stmt = null; 
	LinkedList<String[]> allSQL= null; 
	//private String sqls[]=null;
	private int id=0;

	public ConcurrentTest(int i,LinkedList<String[]> all){
		id=i;
		allSQL=all;
		//if(sqls.length==1)
			//System.out.println("thread " + id + ": " + sqls[0] + " : "+ urls[id%urls.length]);
	}
	
	private static LinkedList<String> readFile(String fileName) {
		File file = new File(fileName);  
        BufferedReader reader = null;  
        LinkedList<String> cmds = new LinkedList<String>();
        try { 
            reader = new BufferedReader(new FileReader(file));  
            String tempString = null;  
            
           // int line = 1;  
            while ((tempString = reader.readLine()) != null) {  
                //System.out.println("line " + line + ": " + tempString);  
                //line++;
                tempString = tempString.trim();
                //if it is not comments, add it to cmds
                if(!tempString.startsWith("--") && tempString.length() > 0)
                	cmds.add(tempString);
            }  
            reader.close();  
        } catch (IOException e) {  
        	System.out.println("failed to read sql file: " + e.getMessage());
        	System.exit(-1);
        } finally {  
            if (reader != null) {  
                try {  
                    reader.close();  
                } catch (IOException e1) {  
                }  
            }  
        }  
        return cmds;
	}
	
	private static LinkedList<String[]> paseCMD(LinkedList<String> allCMD) { 
        LinkedList<String[]> ALLSQL = new LinkedList<String[]>();
        String sql = null;
        for(int i=0;i<allCMD.size();i++) {
        	sql=allCMD.get(i);
        	sql= sql.trim();
    		//System.out.println(id + "," + sql);	
    		String sqlTasks[]=sql.split(";");
    		ALLSQL.add(sqlTasks);
        }
 
        return ALLSQL;
	}
	private static boolean initProp() {
		String confFile ="cctest.conf";
		try {
			properties.load(new FileInputStream(confFile));
		}catch(Exception e) {
			System.out.println("failed to load " + confFile + ": " + e.getMessage());
			return false;
		}
		try {
			dbType=properties.getProperty("dbType");
			urls=properties.getProperty("urls").split("#");
			userName=properties.getProperty("userName");
			password=properties.getProperty("password");
			runTime=Long.parseLong(properties.getProperty("runTime"));
			numOfConn=Integer.parseInt(properties.getProperty("numOfConn"));
			sqlFile=properties.getProperty("sqlFile");
			initSQL=properties.getProperty("initSQL");
			String tmp = properties.getProperty("fixedSQL");
			if(tmp != null && tmp.equalsIgnoreCase("true")) {
				fixedSQL=true;
			}
			System.out.println("initSQL=" + initSQL);
			interval=Integer.parseInt(properties.getProperty("interval"));
			if(interval < 0)
				interval = 0;
		}catch(Exception e) {
			System.out.println("failed to parse parameter: " + e.getMessage());
			return false;
		}
				
        cmds = readFile(sqlFile);        
		return true;		
	}
		
	public static void main(String[] args) {
		System.out.println("version:" + version);
		//step 1
		initProp();
		
		//step 2
		//use the following codes to test.
//		String SQLs=" create table cc (a1 int, a2 int);" + "\n" +
//				"alter table cc drop column a2;"  + "\n" +
//				"alter table cc add column a2 int;"  + "\n" +
//				"drop table if exists cc;"  + "\n" +
//				"drop table cc;"  + "\n" +
//				"create index cc_idx on cc(a1);"  + "\n" +
//				"drop index cc_idx;"  + "\n" +
//				"select * from cc;" + "\n" +
//				"insert into cc values(1,1);insert into cc values(3,3);insert into cc values(5,5);commit;" + "\n" +
//				"insert into cc values(7,1);insert into cc values(9,3);insert into cc values(11,5);commit;" + "\n" +
//				"delete from cc;" + "\n" +
//				"truncate table cc;" + "\n" +
//				"analyzE cc;" ;
//				//"vacuum full cc;" + "\n" + //not supported
//				
//		String cmd[]= SQLs.split("\n");
//		cmds = new LinkedList<String>();
//		for(int i=0;i<cmd.length;i++) {
//			cmds.add(cmd[i]);
//		}
		
		//step 3
		if(cmds.size()<=0) {
			System.out.println("no sql to be executed, exiting now");
			return;
		}
		//numOfConn=0 means conn:sql=1:1
		if(numOfConn==0)
			numOfConn = cmds.size();
		

		LinkedList<String[]> sqls = paseCMD(cmds);
		for(int i=0;i<=numOfConn;i++) {
				new ConcurrentTest(i,sqls).start();
		}
	}
	
	public void monitor() {
		long endTime = System.currentTimeMillis() + runTime * 1000;
		long pre = count2.get();
		long curr = 0;
		while(System.currentTimeMillis() <= endTime) {
				try {
					Thread.sleep(3 * 1000);					
				} catch (InterruptedException e) {
					//
				}
				curr = count2.get();				
				System.out.println("tps = "+ (curr - pre)/3 + ",total = "+ curr + ", " + System.currentTimeMillis());
				pre = curr;
		}
		needStop = true;
		System.out.println("test finished: total = "+ curr + ", " + System.currentTimeMillis());
	}
	
	private boolean dbInit(boolean autoCommit) {
		while(!needStop) {
			conn = getConn(urls[id%urls.length]);	
			if(conn == null) {
				try {
					Thread.sleep(interval);
				}catch(InterruptedException  ie) {					
				}
				continue;
			}

			try {
				conn.setAutoCommit(autoCommit);
				stmt=conn.createStatement();				
				if(initSQL != null && !initSQL.equals(""))
					stmt.execute(initSQL);
				
				return true;
			} catch (Exception e) {
				System.out.println(e.getMessage());
				freeDBResource();
			} 
		}
		return false;
	}
	
	public void run(){		
		//last one is the monitor
		if(id == numOfConn) {
			monitor();
			return;
		}
		
		String sqlTasks[]= null;
		if(fixedSQL)
			sqlTasks=allSQL.get(id);
		
		//print task info
//		String msg = id + "," + sqlTasks.length + ",";
//		for(int i=0;i<sqlTasks.length;i++)
//			msg = msg + sqlTasks[i] + ",";
//		System.out.println( msg);
		
		dbInit(true); //default is auto commit
		int index=0;
		int i=0;
		
		while(!needStop) {
			if(!fixedSQL) {
				sqlTasks = allSQL.get(index++%allSQL.size());
			}
			
			boolean commit = true; 
			int sqlCount=1;
			if(sqlTasks.length>1) {
				String lastCmd = sqlTasks[sqlTasks.length - 1].trim();
				if(lastCmd.equalsIgnoreCase("rollback")) {
					commit = false;
					sqlCount = sqlTasks.length -1;
				}
				else if(lastCmd.equalsIgnoreCase("commit")) {
					commit =true;
					sqlCount = sqlTasks.length -1;
				}
				else {
					//don't have txn control statement, default is commit   
					commit = true;
					sqlCount = sqlTasks.length ;
				}			
			}
			
			if(sqlCount <=0) {
				System.out.println("thread " + id + ": "+ sdf.format(new Date()) +  ": no sql to be executed, exiting" );
				return;
			}

			
			try {
				if(sqlTasks.length == 1) {
					stmt.execute(sqlTasks[0]);
				}
				else{
					conn.setAutoCommit(false);
					for(i=0;i<sqlCount;i++) {
						stmt.execute(sqlTasks[i]);
					}
					if(commit)
						conn.commit();
					else
						conn.rollback();					
				}
				count2.getAndIncrement();
			}catch(Exception e) {
				System.out.println("thread " + id + ": "+ sdf.format(new Date()) +  ": " + e.getMessage() +", " + sqlTasks[i]);
				e.printStackTrace();
				try {
					conn.rollback();
				}catch(Exception ee) {					
				}
				
				try {
					stmt.execute("select 1");
				}catch(Exception ee) {
					System.out.println("thread " + id + ": "+ sdf.format(new Date()) +  ": " + "refresh connection due to " + ee.getMessage());
					//conn is invalid, refresh
					freeDBResource();
					dbInit(sqlTasks.length<=1);
				}
			}
		}
	}
	
	
	public void freeDBResource()
	{
		try{
			if(stmt != null)
				stmt.close();
		}catch(Exception e){
		}
		stmt=null;
		
		try{
			if(conn != null)
				conn.close();
		}catch(Exception e){
		}
		conn=null;
	}
	
	public static Connection getConn(String url){
		try {
			if(dbType.equalsIgnoreCase("oracle")) {
				 Class.forName("oracle.jdbc.driver.OracleDriver");
			}else if(dbType.equalsIgnoreCase("postgres")) {
				Class.forName("org.postgresql.Driver");
			}else if(dbType.equalsIgnoreCase("gaussdb")) {
				Class.forName("com.huawei.gauss200.jdbc.Driver");
			}else {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
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
            System.out.println("error, connect failed: " +url);
            e.printStackTrace();
        }
        
        return con;
	}	
}
