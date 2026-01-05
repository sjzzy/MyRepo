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

public class ConsitencyTest extends Thread {
	// 出了不一致等问题后，需要停止测试，以保留现场，方便问题定位
	// 这里不需要准确的控制，即使有延时，也无所谓，所以为了减少工具层的开销，不使用 volatile
	private static boolean needStop = false;

	// 支持不同的测试模式
	static enum MODE {
		// TODO 后续支持SINGLE_MERGEINTO
		FOR_UPDATE, UPDATE, UPSERT, UPSERT_WITH_FOR_UPDATE, SINGLE_UPDATE, MERGEINTO, SINGLE_MERGEINTO;
	}

	private static ConsitencyTest.MODE mode = ConsitencyTest.MODE.FOR_UPDATE;

	// 增加子事务的测试，设置重试的最大次数
	private static int max_retry = 10;

	// TODO, 后续支持不同的编码，优先级低。当前不生效。
	private static String encoding = "utf-8";

	// 是否开启debug模式，增加打印信息，方便调式
	private static boolean debug = false;

	// 最大的用户id，用于调整数据量
	private static int max_userid = 10000;

	// 数据库类型：Gaussdb，oracle
	private static String dbType = "gaussdb";
	private static String validateSQL = "";// 用于校验连接是否正常

	// 调整更新的列数
	private static int update_columns = 1;

	// 多少笔转账，组成一个事务，用于调整事务的大小；增大后，可以验证死锁的场景
	private static int transaction_size = 1;

	// 数据分布方式，当前不生效，优先级低
	private static String distribution = "uniform";

	// 是否开启长事务，如果开启，校验线程的事务，持续不提交
	private static boolean long_transaction = false;

	private final static long UNKNOW_BALANCE = -99999999L;

	// 数据库的用户名、密码
	private String username = null;
	private String password = null;

	// 为了及时检测问题，启动一个线程持续做数据一致性验证。
	private boolean isChecker = false;

	private Connection conn = null;
	private String url = null;
	private Statement stmt = null;
	private PreparedStatement[] transStmt = null;
	private PreparedStatement[] selectStmt = null;
	private PreparedStatement logStmt = null;
	private PreparedStatement logStmtPart = null;

	private PreparedStatement checkStmt = null;
	private long expectedTotalBalance = -1;

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private long threadId = 0L;

	// 用于记录运行节点的信息，尤其是在分布式场景下
	private String node_name = null;

	// 统计事务总数
	private static AtomicLong txnCounter = new AtomicLong(0);

	public String getCurrentTime() {
		return sdf.format(new Date());
	}

	// 设置测试运行模式，控制事务的SQL语句
	public static void setMode(int m) {
		switch (m) {
		case 1:
			mode = ConsitencyTest.MODE.UPDATE;
			break;
		case 2:
			mode = ConsitencyTest.MODE.UPSERT;
			break;
		case 3:
			mode = ConsitencyTest.MODE.UPSERT_WITH_FOR_UPDATE;
			break;
		case 4:
			mode = ConsitencyTest.MODE.SINGLE_UPDATE;
			break;
		case 5:
			mode = ConsitencyTest.MODE.MERGEINTO;
			break;
		default:
			mode = ConsitencyTest.MODE.FOR_UPDATE;
		}

		System.out.println("Test mode is " + mode);
	}

	public static void setEncoding(String enc) {
		encoding = enc;
	}

	public static void setDbType(String db) {
		assert (db != null);
		dbType = db;
		if (dbType.equalsIgnoreCase("gaussdb"))
			validateSQL = "select 1";
		else
			validateSQL = "select 1 from dual ";
	}

	public static void setMaxRetry(int retry) {
		max_retry = retry;
	}

	private ConsitencyTest(int id, String v_url, String user, String pwd, boolean isChecker) {
		this.isChecker = isChecker;
		this.threadId = id;

		this.url = v_url;
		this.username = user;
		this.password = pwd;
	}

	private boolean initDBHandler() {
		try {
			this.conn = getConn(this.url);
			if (this.conn == null) {
				return false;
			}
			this.node_name = null;

			this.conn.setAutoCommit(false);
			this.stmt = this.conn.createStatement();

			if (dbType.equalsIgnoreCase("gaussdb")) {
				ResultSet rs = this.stmt.executeQuery("select pgxc_node_str()");
				if (rs.next())
					this.node_name = rs.getString(1);
				rs.close();
			} else {
				this.node_name = dbType;
			}
			this.selectStmt = new PreparedStatement[update_columns];
			this.transStmt = new PreparedStatement[update_columns];
			if (mode == ConsitencyTest.MODE.FOR_UPDATE)
				for (int i = 0; i < update_columns; i++) {
					this.selectStmt[i] = this.conn
							.prepareStatement("select balance" + i + " from test where id = ? for update");
					this.transStmt[i] = this.conn
							.prepareStatement("update test set balance" + i + " = ? + (?) where id = ?");
				}
			else if (mode == ConsitencyTest.MODE.UPDATE)
				for (int i = 0; i < update_columns; i++) {
					this.selectStmt[i] = this.conn.prepareStatement("select ?"); // not used
					this.transStmt[i] = this.conn
							.prepareStatement("update test set balance" + i + " = balance" + i + " + (?) where id= ?");
				}
			else if (mode == ConsitencyTest.MODE.SINGLE_UPDATE) {
				for (int i = 0; i < update_columns; i++) {
					this.selectStmt[i] = this.conn.prepareStatement("select ?"); // not used

					// 在分布式环境下，不能直接下推，走pgxc计划
					this.transStmt[i] = this.conn.prepareStatement("update test a set a.balance" + i
							+ " = case a.id when ? then a.balance" + i + " - (?) when ? then a.balance" + i
							+ " + (?) end from test2 b where a.id in (?,?) and a.id=b.id");
				}
			} else if (mode == ConsitencyTest.MODE.MERGEINTO) {
				for (int i = 0; i < update_columns; i++) {
					this.selectStmt[i] = this.conn.prepareStatement("select ?");
					this.transStmt[i] = this.conn.prepareStatement(
							"merge into test using (select ?,?) as t(balance,id) on test.id=t.id when matched then update set test.balance"
									+ i + " = test.balance" + i + " + (t.balance) "
									+ "when not matched then insert(id,balance" + i + " ) values(t.id, t.balance)");
				}
			} else {
				// UPSERT or UPSERT_WITH_FOR_UPDATE
				for (int i = 0; i < update_columns; i++) {
					this.selectStmt[i] = this.conn
							.prepareStatement("select balance" + i + " from test where id= ? for update");
					this.transStmt[i] = this.conn.prepareStatement("insert into test(id,balance" + i
							+ ") values(?,?) on duplicate key update balance" + i + "= ? + balance" + i);
				}
			}
			this.logStmt = this.conn.prepareStatement(
					"insert into log_all(actfrom,fromblc,actto,toblc,num,threadid,seq,retry,index,nodename,tran) values( ?,?,?,?,?,?,?,?,?,?,?)");

			this.logStmtPart = this.conn.prepareStatement(
					"insert into log_part(op,act_No,old_blc,num,threadid,index,tran) values( ?,?,?,?,?,?,?)");

			String sql = "select sum(balance0) ";
			for (int i = 1; i < update_columns; i++) {
				sql = sql + ",sum(balance" + i + ") ";
			}
			sql = sql + " from test ";
			this.checkStmt = this.conn.prepareStatement(sql);

			if (this.isChecker) {
				// 启用并行查询
				this.stmt.execute("set query_dop=8;");
			}

			return true;
		} catch (Exception e) {
			System.out.println("refresh conn & init db resources: " + e.getMessage());
			if (conn != null) {
				freeConn();
			}
		}
		return false;
	}

	public boolean checkTotalBalance() throws SQLException {
		ResultSet rs = null;
		rs = this.checkStmt.executeQuery();
		if (rs.next()) {
			for (int k = 1; k <= update_columns; k++) {
				long ret2 = rs.getLong(k);
				// expectedResult此时可能准确赋值，无所谓，下面还会重新赋值；之所以先判断是否相等再判断是否赋值，是因为绝大部分都是相等的，减少不必要的运算
				if (ret2 == expectedTotalBalance)
					continue;
				if (expectedTotalBalance < 0) {
					this.expectedTotalBalance = ret2;
					continue;
				}

				System.err.println("incorrect total balance: " + ret2 + ":" + getCurrentTime() + ":"
						+ this.checkStmt.toString() + ":column" + (k - 1));
				needStop = true;
				return false;
			}
		} else {
			System.err.println("no data found: " + getCurrentTime() + ":" + this.checkStmt.toString());
			needStop = true;
			return false;
		}
		rs.close();
		rs = null;
		return true;
	}

	// 增加监控，持续打印tps
	public void checkTps() {
		long pre = 0;
		long curr = 0L;

		while (!needStop) {
			pre = txnCounter.get();

			try {
				Thread.sleep(3000L);
			} catch (Exception localException) {
				System.err.println(localException.getMessage() + ":" + getCurrentTime());
			}
			curr = txnCounter.get();
			System.out.println("tps " + (curr - pre) / 3L + ", " + getCurrentTime());
		}
	}

	public void run() {
		int ret = 0;
		this.threadId = getId();
		Random rand = new Random(this.threadId);
		while (true) {
			if (initDBHandler())
				break;
			try {
				Thread.sleep(1000L);
			} catch (Exception exception) {
			}
		}

		if (this.isChecker) {
			checkTps();
			return;
		}

		long from = 0L;
		long to = 0L;
		long num = 0L;
		long toBalance = 0L;
		long fromBalance = 0L;
		int index = 0;
		int retry_count = 0;
		ResultSet rs = null;
		long seq = 0L;

		while (!needStop) {
			seq++;
			try {
				for (int tran = 0; tran < transaction_size; tran++) {
					from = rand.nextInt(max_userid) + 1;
					do {
						to = rand.nextInt(max_userid) + 1;
					} while (from == to);

					num = rand.nextInt(1000000000);
					// 随机选择更新一列，覆盖不同的表宽度
					index = rand.nextInt(update_columns);
					toBalance = 0L;
					fromBalance = 0L;
					retry_count = 0;

					// 执行转出操作
					for (int retry = 0; retry < 10000; retry++) { // 进入重试逻辑，只能通过触发SQLException
						try {
							if (debug) {
								checkTotalBalance();
							}

							// 每个转账语句，最多触发一次子事务
							if ((retry == 0) && (max_retry > 0)) {
								String savepoint = "savepoint sp_minus" + seq + "_" + retry + "_" + tran;
								if (debug)
									System.out.println(savepoint);
								this.stmt.execute(savepoint);
							}

							if (mode == ConsitencyTest.MODE.UPDATE) {
								this.transStmt[index].setLong(1, 0L - num);
								this.transStmt[index].setLong(2, from);
								// 没有获取原始值，用特殊值 -99999999代替。
								fromBalance = UNKNOW_BALANCE;
							} else if (mode == ConsitencyTest.MODE.MERGEINTO) {
								this.transStmt[index].setLong(1, 0L - num);
								this.transStmt[index].setLong(2, from);
								fromBalance = UNKNOW_BALANCE;
							} else if (mode == ConsitencyTest.MODE.FOR_UPDATE) {
								this.selectStmt[index].setLong(1, from);
								rs = this.selectStmt[index].executeQuery();
								if (rs.next()) {
									fromBalance = rs.getLong(1);
								} else {
									needStop = true;
									throw new Exception(
											"no data found:" + this.selectStmt[index].toString() + ",id=" + from);
								}

								rs.close();
								rs = null;
								this.transStmt[index].setLong(1, fromBalance);
								this.transStmt[index].setLong(2, 0L - num);
								this.transStmt[index].setLong(3, from);
							} else if (mode == ConsitencyTest.MODE.SINGLE_UPDATE) {
								this.transStmt[index].setLong(1, from);
								this.transStmt[index].setLong(2, num);
								this.transStmt[index].setLong(3, to);
								this.transStmt[index].setLong(4, num);
								this.transStmt[index].setLong(5, from);
								this.transStmt[index].setLong(6, to);
								fromBalance = UNKNOW_BALANCE;
								// 只有一条语句，更新两个账号，下面没有处理，在此处统一赋值
								toBalance = UNKNOW_BALANCE;
							} else {
								if (mode == ConsitencyTest.MODE.UPSERT_WITH_FOR_UPDATE) {
									this.selectStmt[index].setLong(1, from);
									rs = this.selectStmt[index].executeQuery();
									if (rs.next()) {
										fromBalance = rs.getLong(1);
									} else {
										needStop = true;
										throw new Exception(
												"no data found:" + this.selectStmt[index].toString() + ",id=" + from);
									}

									rs.close();
									rs = null;
								} else {
									fromBalance = UNKNOW_BALANCE;
								}

								this.transStmt[index].setLong(1, from);
								this.transStmt[index].setLong(2, 0L);
								this.transStmt[index].setLong(3, 0L - num);
							}

							ret = this.transStmt[index].executeUpdate();

							if (((ret != 1) && (mode != ConsitencyTest.MODE.SINGLE_UPDATE))
									|| ((mode == ConsitencyTest.MODE.SINGLE_UPDATE) && (ret != 2))) {
								needStop = true;
								throw new Exception("incorrect update:" + ret + ":" + this.transStmt[index].toString());
							}
							this.logStmtPart.setString(1, "m");
							this.logStmtPart.setLong(2, from);
							this.logStmtPart.setLong(3, fromBalance);
							this.logStmtPart.setLong(4, 0L - num);
							this.logStmtPart.setLong(5, this.threadId);
							this.logStmtPart.setInt(6, index);
							this.logStmtPart.setInt(7, tran);
							ret = this.logStmtPart.executeUpdate();
							if (ret == 1)
								break;
							else {
								needStop = true;
								throw new Exception("incorrect insert:" + ret + ":" + this.logStmtPart.toString());
							}
						} catch (SQLException e) {
							// 进入重试逻辑，只能通过触发SQLException
							System.err.println(e.getMessage());
							// e.printStackTrace();
							if (retry >= max_retry)
								throw e;
							retry_count++;
							if (max_retry > 0)
								this.stmt.execute("rollback to savepoint sp_minus" + seq + "_" + 0 + "_" + tran);
						}
					}

					// 执行转入操作
					for (int retry = 0; retry <= max_retry; retry++) {
						try {
							if ((retry == 0) && (max_retry > 0)) {
								this.stmt.execute("savepoint sp_add" + seq + "_" + retry + "_" + tran);
							}
							if (mode == ConsitencyTest.MODE.UPDATE) {
								this.transStmt[index].setLong(1, num);
								this.transStmt[index].setLong(2, to);
								toBalance = UNKNOW_BALANCE;
							} else if (mode == ConsitencyTest.MODE.MERGEINTO) {
								this.transStmt[index].setLong(1, num);
								this.transStmt[index].setLong(2, to);
								toBalance = UNKNOW_BALANCE;
							} else if (mode == ConsitencyTest.MODE.FOR_UPDATE) {
								this.selectStmt[index].setLong(1, to);
								rs = this.selectStmt[index].executeQuery();
								if (rs.next())
									toBalance = rs.getLong(1);
								else {
									needStop = true;
									throw new Exception(
											"no data found:" + this.selectStmt[index].toString() + ",id=" + to);
								}
								rs.close();
								rs = null;

								this.transStmt[index].setLong(1, toBalance);
								this.transStmt[index].setLong(2, num);
								this.transStmt[index].setLong(3, to);
							} else {
								if (mode == ConsitencyTest.MODE.UPSERT_WITH_FOR_UPDATE) {
									this.selectStmt[index].setLong(1, to);
									rs = this.selectStmt[index].executeQuery();
									if (rs.next())
										toBalance = rs.getLong(1);
									else {
										needStop = true;
										throw new Exception(
												"no data found:" + this.selectStmt[index].toString() + ",id=" + to);
									}
									rs.close();
									rs = null;
								} else {
									// UPSERT、SINGLE_UPDATE
									toBalance = UNKNOW_BALANCE;
								}

								this.transStmt[index].setLong(1, to);
								this.transStmt[index].setLong(2, 0L);
								this.transStmt[index].setLong(3, num);
							}
							if (mode != ConsitencyTest.MODE.SINGLE_UPDATE) {
								ret = this.transStmt[index].executeUpdate();
								if (ret != 1) {
									needStop = true;
									throw new Exception(
											"incorrect update:" + ret + ":" + this.transStmt[index].toString());
								}
							}
							this.logStmtPart.setString(1, "a");
							this.logStmtPart.setLong(2, to);
							this.logStmtPart.setLong(3, toBalance);
							this.logStmtPart.setLong(4, num);
							this.logStmtPart.setLong(5, this.threadId);
							this.logStmtPart.setInt(6, index);
							this.logStmtPart.setInt(7, tran);
							ret = this.logStmtPart.executeUpdate();
							if (ret <= 0) {
								needStop = true;
								throw new Exception("incorrect insert:" + ret + ":" + this.logStmtPart.toString());
							}

							this.logStmt.setLong(1, from);
							this.logStmt.setLong(2, fromBalance);
							this.logStmt.setLong(3, to);
							this.logStmt.setLong(4, toBalance);
							this.logStmt.setLong(5, num);
							this.logStmt.setLong(6, this.threadId);
							this.logStmt.setLong(7, seq);
							this.logStmt.setInt(8, retry_count);
							this.logStmt.setInt(9, index);
							this.logStmt.setString(10, this.node_name);
							this.logStmt.setInt(11, tran);
							ret = this.logStmt.executeUpdate();
							if (ret == 1)
								break;
							else {
								needStop = true;
								throw new Exception("incorrect insert:" + ret + ":" + this.logStmt.toString());
							}
						} catch (SQLException e) {
							System.out.println(e.getMessage());
							if (retry >= max_retry)
								throw e;
							retry_count++;
							if (max_retry > 0)
								this.stmt.execute("rollback to savepoint sp_add" + seq + "_" + 0 + "_" + tran);
						}
					}
				}

				this.conn.commit();
				txnCounter.getAndIncrement();
			} catch (Exception e2) {
				System.out.println("error: " + getCurrentTime() + "," + e2.getMessage());
				// e2.printStackTrace();
				try {
					this.conn.rollback();
				} catch (Exception localException1) {
				}

				try {
					this.stmt.executeQuery(validateSQL);
				} catch (Exception e) {
					// 重建连接，直到成功
					freeConn();
					System.out.println(getCurrentTime() + ", will refresh conn: " + e.getMessage());
					while (true) {
						if (initDBHandler())
							break;
						try {
							Thread.sleep(3000L);
						} catch (InterruptedException se) {
						}
					}
				}
			} catch (Throwable t) {
				// 该线程退出执行
				System.out
						.println(getCurrentTime() + "thread " + this.threadId + " will exit, due to " + t.getMessage());
				t.printStackTrace();
				break;
			}
		}

		System.out.println(
				getCurrentTime() + "thread " + this.threadId + " will exit: needstop=" + needStop + ",seq=" + seq);
		try {
			this.stmt.close();
		} catch (SQLException localSQLException1) {
		}

		for (int n = 0; n < update_columns; n++) {
			try {
				this.selectStmt[n].close();
			} catch (SQLException localSQLException2) {
			}
			try {
				this.transStmt[n].close();
			} catch (SQLException localSQLException3) {
			}
		}
		try {
			this.checkStmt.close();
		} catch (SQLException localSQLException4) {
		}
		try {
			this.logStmt.close();
		} catch (SQLException localSQLException5) {
		}
		freeConn();
	}

	public static void main(String[] args) {
		int id = 10000;
		int parallel = 1000;
		String username = null;
		String password = null;
		String[] urlArray = null;
		String usage = "url[,url] max_id parallel username password mode encoding debug max_retry_count db_type update_columns transaction_size distribution long_transaction";
		if (args.length != usage.split(" ").length) {
			System.out.println("version 4.3\nusage:" + usage);
			return;
		}

		urlArray = args[0].split("#");
		try {
			id = Integer.parseInt(args[1]);
		} catch (Exception e) {
			System.out.println(
					"error occured while parsing parameter, use default max_id:" + id + "\n detail:" + e.getMessage());
		}
		try {
			parallel = Integer.parseInt(args[2]);
			setMaxRetry(Integer.parseInt(args[8]));
		} catch (Exception e) {
			System.out.println("error occured while parsing parameter, use default parallel:" + parallel + "\n detail:"
					+ e.getMessage());
		}

		username = args[3];
		password = args[4];
		setMode(Integer.parseInt(args[5]));
		setEncoding(args[6]);
		debug = Boolean.parseBoolean(args[7]);
		setDbType(args[9]);
		if (debug) {
			System.out.println("username=" + username);
			System.out.println("password=" + password);
			System.out.println("urls=" + args[0]);
			System.out.println("parallel=" + username);
			System.out.println("username=" + parallel);
			System.out.println("max_retry=" + max_retry);
		}

		update_columns = Integer.parseInt(args[10]);
		transaction_size = Integer.parseInt(args[11]);
		distribution = args[12];
		long_transaction = Boolean.parseBoolean(args[13]);

		int index = 0;

		for (int i = 0; i <= parallel; i++) {
			if (i == 0) {
				new ConsitencyTest(0, urlArray[0], username, password, true).start();
			} else {
				index = i % urlArray.length;
				new ConsitencyTest(id, urlArray[index], username, password, false).start();
				try {
					Thread.sleep(10L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void freeConn() {
		if (this.conn != null) {
			try {
				this.conn.close();
			} catch (Exception localException) {
			}
			this.conn = null;
		}
	}

	public Connection getConn(String url) {
		try {
			if (dbType.equalsIgnoreCase("oracle"))
				Class.forName("oracle.jdbc.driver.OracleDriver");
			else if (dbType.equalsIgnoreCase("gaussdb"))
				Class.forName("com.huawei.gaussdb.jdbc.Driver");
			else
				Class.forName("com.huawei.gaussdb.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("error, can not find driver");
			e.printStackTrace();
			return null;
		}

		Connection con = null;
		try {
			con = DriverManager.getConnection(url, this.username, this.password);
		} catch (SQLException e) {
			System.out.println("error, connect failed: " + url);
			e.printStackTrace();
		}

		return con;
	}

}
