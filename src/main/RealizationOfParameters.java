package main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Properties;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import tool.ReadXML;


public class RealizationOfParameters {

	public final int MAX_LINE = 1024;					/* 最大脚本行数*/
	public final int MAXCLIENTS = 1024;             	/* 最大客户端数目*/
	public final int LOG_STEP_SECONDS = 5;				/* 秒之间的日志消息 */
	public final int DEFAULT_NXACTS = 10;				/* 默认的 nxacts */
	public final int MAX_SCRIPTS = 128;					/* 脚本允许的最大数 */
	public final int MAX_VAR = 50;						/* 最大变量个数 */
	public final String WSEP = "@";						/* weight分隔符 */
	
	public boolean is_init_mode = false;               	/* 初始化模式？ */
	public boolean benchmarking_option_set = false;		/* 一些指定的选项不能用于初始化(-i)模式 */
	boolean is_no_vacuum = false;                     	/* 在不vacuum之前测试 */
	boolean initialization_option_set = false;			/* 一些指定的选项不能用于基准测试模式 */
	boolean	unlogged_tables = false;					/* 使用无日志临时表? */
	boolean foreign_keys = false;						/* 创建外键？ */
	boolean use_quiet = false;							/* 日志模式切换为静默方式。默认是1000条记录打印一条 */
	boolean is_connect = false;							/* 为每个事务建立连接 */
	boolean internal_script_used = false;			
	boolean	latency_limit = false;						/*   */
	boolean	per_script_stats = false;					/* 是否每个脚本收集数据 */
	boolean	is_latencies = false;						/* 各命令延迟报告 */
	boolean script_is_set = false;						/* 脚本是否已经填写 */
	boolean	debug = false;								/* 调试? */
	boolean	use_log;									/* log transaction latencies to a file */
	boolean	do_vacuum_accounts = false; 				/* do vacuum accounts before testing? */
	
	int num_scripts;									/* number of scripts in sql_script[] */
	int duration = 0;									/* duration in seconds */
	int nxacts = 0;										/* 每个客户的事务数量 */
	int log_interval = 1;								/* 用于追踪运行时间和估计的剩余时间 */
	int scale = 1;										/* 插入数据的比例因子 */
	int fillfactor = 100;								/* 设置填充因数，会影响系统性能(这里规定是10-100的数字) */
	public int nbranches = 1;							/* pgbench_branches初始化的数据 */
	public int ntellers	= 10;							/* pgbench_tellers初始化的数据 */
	public int naccounts = 100000;						/* pgbench_accounts初始化的数据 */
	public int nclients = 1;                         	/* 默认的客户数目 */
	public int nthreads = 1;                         	/* 每个客户运行的线程 */
	double total_weight = 0;							/* 脚本比例 */
	public String weight;								/* 脚本占的比例 */
	int num_var;										/* -D 设置的变量名的个数 */
	double	sample_rate = 0.0;							/* log sampling rate (1.0 = log everything, 0.0 = option not given) */
	int clients_id;										/* 客户id */
	int	agg_interval = 0;								/* log aggregates instead of individual transactions */
	int	progress = 0;									/* 每秒显示一次进度报告 */
	long throttle_delay = 0;							/* 0是默认的,意味着没有节流 */
	
	
	Logger logger;										/* 日记变量 */
	String index_tablespace = null;						/* 表空间索引 */
	String tablespace = null;							/* 表空间 */
	
	String querymode = QueryMode.QUERY_SIMPLE.name;		/* simple query模式 */
	
	
	String[][] sql_script = new String[MAX_SCRIPTS][4]; /* 脚本文件 */
	int[] scriptRuntims = new int[MAX_SCRIPTS];			/* 存储每个脚本运行次数 */
	String[] variable = new String[MAX_VAR];			/* -D 设置的变量名 */
	String[] variableName =new String[MAX_VAR];			/* -D 设置的变量值 */
	long[][] CState = new long[MAX_SCRIPTS+1][8];		/* 0:单独脚本运行事务的时间(average) 1:单独脚本运行事务平方时间(stddev) 
	 													 * 2:脚本总花费时间  3:连接总花费时间  4:完成事务次数  5:脚本总花费平方时间 
	 													 * 6:事务的时间最小值   7:事务的时间最大值 */
	long each_sql_total_time[][] = new long[MAX_SCRIPTS][MAX_LINE];	/*每条sql花费的时间 */
	
	String pghost = "";
	String pgport = "";
	String databaseName = "";
	PreparedStatement pstmt = null;
	Connection conn =null;
	int aid = (int) Math.floor(Math.random()*(naccounts*scale)+1);
	int bid = (int) Math.floor(Math.random()*(nbranches*scale)+1);
	int tid = (int) Math.floor(Math.random()*(ntellers*scale)+1);
	int[] temp = {1, -1}; //随即取1或-1出来乘
	int delta = (int)(Math.random()*5000)*temp[(int)(Math.random()*2)];
	

	String[][] BuiltinScript = new String[1][2];//储存临时脚本
	
	String[][] builtin_script = new String[][]{//默认脚本
			{
				"tpcb-like",
				"<builtin: TPC-B (sort of)>",
				"BEGIN;\n"+
				"UPDATE pgbench_accounts SET abalance = abalance + "+delta+" WHERE aid = "+aid+";\n"+
				"SELECT abalance FROM pgbench_accounts WHERE aid = "+aid+";\n"+
				"UPDATE pgbench_tellers SET tbalance = tbalance + "+delta+" WHERE tid = "+tid+";\n"+
				"UPDATE pgbench_branches SET bbalance = bbalance + "+delta+" WHERE bid = "+bid+";\n"+
				"INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES ("+tid+", "+bid+", "+aid+", "+delta+", CURRENT_TIMESTAMP);\n"+
				"END;\n"
			},
			{
				"simple-update",
				"<builtin: simple update>",
				"BEGIN;\n"+
				"UPDATE pgbench_accounts SET abalance = abalance + "+delta+" WHERE aid = "+aid+";\n"+
				"SELECT abalance FROM pgbench_accounts WHERE aid = "+aid+";\n"+
				"INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES ("+tid+", "+bid+", "+aid+", "+delta+", CURRENT_TIMESTAMP);\n"+
				"END;\n"
			},
			{
				"select-only",
				"<builtin: select only>",
				"SELECT abalance FROM pgbench_accounts WHERE aid = "+aid+";\n"
			}
		};
			
	
	public RealizationOfParameters(){}
	
	public RealizationOfParameters(String[] param,Logger logger){
		this.logger = logger;
		if(param.length > 0){
			if("--help".equals(param[0]) || "-?".equals(param[0])){
				usage();
				System.exit(0);
			}
			if("--version".equals(param[0]) || "-V".equals(param[0])){
				System.out.println("bench  (hhdabase) 1.0");
				System.exit(0);
			}
		}
			try {
				realizationOfParameters(param);
			} catch (Exception e) {
				System.out.printf("错误信息：%s \n",e.getMessage());
				System.exit(1);
			}
		
	}
	
	public void realizationOfParameters(String[] param) throws Exception{
		for (int i = 0; i < param.length; i++) {
			switch(param[i]){
			//==================初始化选项==================
				case "-i" :{
					is_init_mode = true;
					break;
				}
				case "-F" :{
					initialization_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid fillfactor(%s): \"%s\"\n",  param[i],param[i+1]);
						System.exit(1);
					}
					fillfactor =Integer.parseInt(param[i+1]);
					if (fillfactor < 10 || fillfactor > 100)
					{
						System.out.println("invalid fillfactor: "+fillfactor+",必须是10-100之间的数字！");
						System.exit(0);
					}
					break;
				}
				case "-s":{
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid scaling factor(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					scale = Integer.parseInt(param[i+1]);
					if (scale <= 0){
						System.out.println("invalid scaling factor: "+scale+",必须是正整数！");
						System.exit(0);
					}
					break;
				}
				case "-q":{
					initialization_option_set = true;
					use_quiet = true;
					break;
				}
				case "-fk":{//foreign_keys
						foreign_keys = true;
						initialization_option_set = true;
					break;
				}
				case "-ut":{//unlogged_tables
						unlogged_tables = true;
						initialization_option_set = true;
					break;
				}
				case "-ts":{//创建表空间 -ts name
					initialization_option_set = true;
					tablespace = param[i+1];
					break;
				}
				case "-it":{//index-tablespace 
					initialization_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("s% 缺少参数 \n",param[i]);
						System.exit(1);
					}
					index_tablespace = param[i+1];
					break;
				}
				//==================基准测试选项==================
				case "-n":{
					is_no_vacuum = true;
					break;
				}
				case "-v":{
					do_vacuum_accounts = true;
					break;
				}
				case "-c":{
					benchmarking_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid number of clients(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					nclients = Integer.parseInt(param[i+1]);
					if (nclients <= 0 || nclients > MAXCLIENTS)
					{
						System.out.printf("invalid number of clients(%s):"+nclients+" \n",param[i]);
						System.exit(0);
					}
					break;
				}
				case "-j":{
					benchmarking_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid number of threads(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					nthreads = Integer.parseInt(param[i+1]);
					if (nthreads <= 0)
					{
						System.out.printf("invalid number of threads(%s):"+nthreads+" \n",param[i]);
						System.exit(0);
					}
					break;	
				}
				case "-t":{
					benchmarking_option_set = true;
					if (duration > 0)
					{
						System.out.printf("specify either a number of transactions (-t) or a duration (-T), not both\n");
						System.exit(0);
					}
					
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid number of transactions(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					nxacts = Integer.parseInt(param[i+1]);
					if (nxacts <= 0)
					{
						System.out.printf("invalid number of transactions(%s): "+nxacts+"\n",param[i]);
						System.exit(0);
					}
					break;
				}
				case "-C":{//每个事务都新创建一个连接来执行，而不是仅使用一个客户端连接。这个对于测试连接过载是有意义的。
					benchmarking_option_set = true;
					is_connect = true;
					break;
				}
				case "-M":{
					benchmarking_option_set = true;
					if (num_scripts > 0)
					{
						System.out.println("query mode (-M) should be specified before any transaction scripts (-f or -b)\n");
						System.exit(0);
					}
					
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					
					int j = 0;
					for (; j < QueryMode.values().length; j++) {
						if (QueryMode.values()[j].name.equals(param[i+1])){
							querymode = QueryMode.values()[j].name;
							break;
						}
					}
					if (j >= QueryMode.values().length)
					{
						System.out.println("invalid query mode (-m): "+param[i+1]);
						System.exit(0);
					}
					break;
				}
				case "-S":{
					process_builtin(findBuiltin("select-only"), "1");
					benchmarking_option_set = true;
					internal_script_used = true;
					break;
				}
				case "-N":{
					process_builtin(findBuiltin("simple-update"), "1");
					benchmarking_option_set = true;
					internal_script_used = true;
					break;
				}
				case "-f":{
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}

					weight = parseScriptWeight(param[i+1]);
					process_builtin(BuiltinScript,weight);
					benchmarking_option_set = true;
					break;
				}
				case "-D":{
					benchmarking_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(param[i+1].contains("=")){
						if(param[i+1].split("=").length < 2){
							System.out.printf("invalid variable definition(%s): \"%s\"\n",param[i],param[i+1]);
							System.exit(1);
						}else{
							variableName[num_var] = ":"+param[i+1].split("=")[0];//变量名
							variable[num_var] = param[i+1].split("=")[1];//变量值
							num_var++;
						}
					}else{
						System.out.printf("invalid variable definition(%s): \"%s\"\n",param[i],param[i+1]);
						System.exit(1);
					}
					script_is_set = true;
					break;
				}
				case "-d":{
					debug = true;
					initLog4j_debug();
					break;
				}
				case "-l":{//日记？
					benchmarking_option_set = true;
					use_log = true;
					
					break;
				}
				case "-sr":{//抽样速率，将数据写入日志时使用，用于降低生产的日志数量。--sampling-rate=rate
					benchmarking_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(NumberUtils.isNumber(param[i+1])){
						sample_rate = Double.parseDouble(param[i+1]);
						if (sample_rate <= 0.0 || sample_rate > 1.0){
							System.err.printf("invalid sampling rate(%s): \"%s\"\n", param[i],param[i+1]);
							System.exit(1);
						}
					}else{
						System.err.printf("invalid sampling rate(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}

					break;	
				}
				case "-ai":{//汇总时间间隔长度。只有-l和这个参数同时使用才有效。日志包含每个间隔的概要（事务数量，最大最小时延和两个额外的用于方差估计的字段）
					benchmarking_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid number of seconds for aggregation(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					agg_interval = Integer.parseInt(param[i+1]);
					if (agg_interval <= 0){
						System.err.printf("invalid number of seconds for aggregation(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					break;
				}
				case "-T":{//运行测试这么多秒,而不是每个客户端运行固定数量的事务。-t - t是互相排斥的
					benchmarking_option_set = true;
					if (nxacts > 0){
						System.err.printf("specify either a number of transactions (-t) or a duration (-T), not both\n");
						System.exit(1);
					}
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数 \n",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid duration(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					duration = Integer.parseInt(param[i+1]);
					if (duration <= 0){
						System.err.printf("invalid duration(%s): \"%s\"\n", param[i],param[i+1]);
						System.exit(1);
					}
					break;
				}
				case "-P":{
					benchmarking_option_set = true;
					if(param.length == (i+1)){
						System.out.printf("%s 缺少参数\n ",param[i]);
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid thread progress delay(%s): \"%s\"\n",param[i],param[i+1]);
						System.exit(1);
					}
					progress = Integer.parseInt(param[i+1]);
					if (progress <= 0)
					{
						System.err.printf("invalid thread progress delay(%s): \"%s\"\n",param[i],param[i+1]);
						System.exit(1);
					}
					break;
				}case "-r":{
		
					benchmarking_option_set = true;
					per_script_stats = true;
					is_latencies = true;
					break;
				}case "-R":{
					if(param.length == (i+1)){
						System.out.printf(param[i]+" 缺少参数\n ");
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid rate limit(%s): \"%s\"\n",param[i],param[i+1]);
						System.exit(1);
					}
					/* get a double from the beginning of option value */
					double	throttle_value = Double.parseDouble(param[i+1]);

					benchmarking_option_set = true;

					if (throttle_value <= 0.0)
					{
						System.err.printf("invalid rate limit(%s): \"%s\"\n",param[i],param[i+1]);
						System.exit(1);
					}
					/* Invert rate limit into a time offset */
					throttle_delay = (long) (1000.0 / throttle_value);
					
					break;
				}
				//==================通用参数==================
				case "-h":{
					if(param.length == (i+1)){
						System.out.printf(param[i]+" 缺少参数 \n");
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid host: \"%s\"\n",param[i+1]);
						System.exit(1);
					}
					pghost = param[i+1];
					String url = ReadXML.getInstance().getUrl();
					try {
						url = url.replace(url.subSequence(url.indexOf("//")+2, url.lastIndexOf(":")), pghost.subSequence(0, pghost.length()));
					} catch (Exception e) {
						throw new Exception("Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.");
					}
						ReadXML.getInstance().setUrl(url);
					break;
				}case "-p":{
					if(param.length == (i+1)){
						System.out.printf(param[i]+" 缺少参数\n ");
						System.exit(1);
					}
					if(!NumberUtils.isNumber(param[i+1])){
						System.err.printf("invalid port: \"%s\"\n",param[i+1]);
						System.exit(1);
					}
					pgport = param[i+1];
					String url = ReadXML.getInstance().getUrl();
					try {
						url = url.replace(url.subSequence(url.lastIndexOf(":")+1, url.lastIndexOf("/")), pgport.subSequence(0, pgport.length()));
					}catch (Exception e) {
						throw new Exception("Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.");
					}
					ReadXML.getInstance().setUrl(url);
					break;
				}case "-U":{
					if(param.length == (i+1)){
						System.out.printf(param[i]+" 缺少参数 \n");
						System.exit(1);
					}
					
					databaseName = param[i+1];
					String url = ReadXML.getInstance().getUrl();
					try {
						url = url.replace(url.subSequence(url.lastIndexOf("/")+1, url.length()), databaseName.subSequence(0, databaseName.length()));
					}catch (Exception e) {
						throw new Exception("Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.");
					}
					ReadXML.getInstance().setUrl(url);
					break;
				}
			}
		}
	
		/* 如果没有指定脚本，就使用默认的脚本 */
		if (num_scripts == 0 && !is_init_mode){
			process_builtin(findBuiltin("tpcb-like"), "1");
			benchmarking_option_set = true;
			internal_script_used = true;
		}
		
		/* 计算脚本的比例 */
		for (int i = 0; i < num_scripts; i++){
			total_weight += Integer.parseInt(sql_script[i][2]);
		}
			
		
		if (total_weight == 0 && !is_init_mode){
			System.out.printf("total script weight must not be zero\n");
			System.exit(0);
		}
		
		/* show per script stats if several scripts are used */
		if (num_scripts > 1){
			per_script_stats = true;
		}
			
		/* 如果没指定nxacts和持续时间，就使用DEFAULT_NXACTS */
		if (nxacts <= 0 && duration <= 0){
			nxacts = DEFAULT_NXACTS;
		}
		
		/* --sampling-rate may be used only with -l */
		if (sample_rate > 0.0 && !use_log){
			System.err.printf("log sampling (--sampling-rate) is allowed only when logging transactions (-l)\n");
			System.exit(1);
		}
		
		/* --sampling-rate may not be used with --aggregate-interval */
		if (sample_rate > 0.0 && agg_interval > 0)
		{
			System.err.printf("log sampling (--sampling-rate) and aggregation (--aggregate-interval) cannot be used at the same time\n");
			System.exit(1);
		}
		
		if (agg_interval > 0 && !use_log)
		{
			System.err.printf("log aggregation is allowed only when actually logging transactions\n");
			System.exit(1);
		}
		
		if (duration > 0 && agg_interval > duration)
		{
			System.err.printf("number of seconds for aggregation (%d) must not be higher than test duration (%d)\n", agg_interval, duration);
			System.exit(1);
		}

		if (duration > 0 && agg_interval > 0 && duration % agg_interval != 0)
		{
			System.err.printf("duration (%d) must be a multiple of aggregation interval (%d)\n", duration, agg_interval);
			System.exit(1);
		}
		
		//线程数目不能大于客户数
		if (nthreads > nclients)
			nthreads = nclients;
		
		/* compute a per thread delay */
		throttle_delay *= nthreads;
		
		if (debug){
			String pghost = ReadXML.getInstance().getUrl().split("//")[1].split(":")[0];
			String pgport = ReadXML.getInstance().getUrl().split("//")[1].split(":")[1].split("/")[0];
			if (duration <= 0){
				System.out.printf("pghost: %s pgport: %s nclients: %d nxacts: %d dbName: %s\n",
						pghost, pgport, nclients, nxacts, ReadXML.getInstance().getSqlName());
			}
			else
				System.out.printf("pghost: %s pgport: %s nclients: %d duration: %d dbName: %s\n",
					   pghost, pgport, nclients, duration, ReadXML.getInstance().getSqlName());
		}
		
		if (is_init_mode){ 
			if (benchmarking_option_set){
				System.out.println("一些指定的选项不能用于初始化(-i)模式");
				System.exit(0);
			}
			init(is_no_vacuum);
			System.exit(0);
		} else {
			if (initialization_option_set){
				System.out.println("一些指定的选项不能用于基准测试模式");
				System.exit(0);
			}
		}
		
		if (!is_no_vacuum){
			try {
				conn = new Operation().createConnection();
				if (conn  == null){
					System.out.println("连接数据库失败！");
					System.exit(0);
				}
			} catch (Exception e) {
				System.out.printf("错误信息：%s \n",e.getMessage());
			}
			System.out.printf("starting vacuum...");
			conn.prepareStatement("vacuum pgbench_branches").execute();
			conn.prepareStatement("vacuum pgbench_tellers").execute();
			conn.prepareStatement("truncate pgbench_history").execute();
			System.out.printf("end.\n");
			if (do_vacuum_accounts)
			{
				System.out.printf("starting vacuum pgbench_accounts...");
				conn.prepareStatement("vacuum analyze pgbench_accounts").execute();
				System.out.printf("end.\n");
			}
		}
		
		//设置脚本变量
		if(script_is_set){
			putVariable();
		}
	}
	
	
	
	public void initLog4j_debug(){
		Properties prop = new Properties();

		prop.setProperty("log4j.rootLogger", "DEBUG, stdout");
		prop.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		prop.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
		prop.setProperty("log4j.appender.stdout.layout.ConversionPattern","%m%n");
	
		PropertyConfigurator.configure(prop);
	}
	
	//设置脚本变量
	public void putVariable(){
		for (int i = 0; i < num_scripts; i++) {
			for (int k = 0; k < num_var; k++) {
				if(sql_script[i][1].contains(variableName[k])){
					sql_script[i][1] = sql_script[i][1].replace(variableName[k], variable[k]);
				}
			}
		}
		
	}
	
	public String parseScriptWeight(String parm){
		try {
			if(parm.contains(WSEP)){
				if(parm.split(WSEP).length < 2){
					System.out.printf("错误信息：%s 缺少比重 \n",parm);
					System.exit(1);
				}
				
				if(NumberUtils.isDigits(parm.split(WSEP)[1])){
					BuiltinScript[0][0] = parm.split(WSEP)[0];//脚本名称
					weight = parm.split(WSEP)[1];//脚本比重
				}else{
					System.out.printf("错误的变量 :%s,必须是非负整数.\n",parm.split(WSEP)[1]);
					System.exit(1);
				}
			}else{
				BuiltinScript[0][0] = parm;//脚本名称
				weight = "1";//脚本比重
			}
		//------读取文件代码-------------
			//1、创建流对象
	        Reader reader=new FileReader(BuiltinScript[0][0]);
	        //构建高效流对象
	        BufferedReader buffReader=new BufferedReader(reader);
	        //2、读取一行字符串
	        String line=buffReader.readLine();
	        StringBuffer buffer=new StringBuffer();
	        while(line!=null){
	            buffer.append(line+"\r\n");
	            line=buffReader.readLine();
	        }
	        BuiltinScript[0][1] = buffer.toString();
	        //3、关闭流
	        buffReader.close();
	        reader.close();
		}  catch (FileNotFoundException e) {
            System.out.println("要读取的文件不存在："+e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.out.println("文件读取错误："+e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.out.println("错误信息："+e.getMessage());
            System.exit(1);
        }
		
		return weight;
	}
	
	public void process_builtin(String[][] BuiltinScript,String weight){
		if (num_scripts >= MAX_SCRIPTS){
			System.out.printf("at most %d SQL scripts are allowed\n", MAX_SCRIPTS);
			System.exit(1);
		}
		sql_script[num_scripts][0] = BuiltinScript[0][0];//脚本名称
		sql_script[num_scripts][1] = BuiltinScript[0][1];//脚本代码
		sql_script[num_scripts][2] = weight;			 //脚本权重
		num_scripts++;
	}
	
	public String[][] findBuiltin(String scriptName){
		int	found = 0;
		for (int i = 0; i < builtin_script.length; i++) {
			if(scriptName.equals(builtin_script[i][0])){
				BuiltinScript[0][0] = builtin_script[i][1];//脚本名称
				BuiltinScript[0][1] = builtin_script[i][2];//脚本代码
				found++;
			}
		}
		
		/* ok, unambiguous result */
		if (found == 1)
			return BuiltinScript;
		
		/* error cases */
		if (found == 0)
			System.out.printf("no builtin script found for name \"%s\"\n", scriptName);
		else	/* found > 1 */
			System.out.printf("ambiguous builtin name: %d builtin scripts found for prefix \"%s\"\n", found, scriptName);
		
		System.exit(1);
		return null;
	}
	
	
	public void init(boolean is_no_vacuum){
		String[][] ddls = new String[][]{
			{
				"pgbench_history",
				"tid int,bid int,aid bigint,delta int,mtime timestamp,filler char(22)"
			},
			{
				"pgbench_tellers",
				"tid int not null,bid int,tbalance int,filler char(84)"
			},
			{
				"pgbench_accounts",
				"aid bigint not null,bid int,abalance int,filler char(84)"
			},
			{
				"pgbench_branches",
				"bid int not null,bbalance int,filler char(88)"
			}
		};
		String[] ddlIndexes = new String[]{
			"alter table pgbench_branches add primary key (bid)",
			"alter table pgbench_tellers add primary key (tid)",
			"alter table pgbench_accounts add primary key (aid)"
		};
		String[] ddlKeys = new String[]{
			"alter table pgbench_tellers add foreign key (bid) references pgbench_branches",
			"alter table pgbench_accounts add foreign key (bid) references pgbench_branches",
			"alter table pgbench_history add foreign key (bid) references pgbench_branches",
			"alter table pgbench_history add foreign key (tid) references pgbench_tellers",
			"alter table pgbench_history add foreign key (aid) references pgbench_accounts"
		};
		
		try {
			conn = new Operation().createConnection();
			if (conn  == null){
				System.out.println("连接数据库失败！");
				System.exit(0);
			}
				
		} catch (Exception e) {
			System.out.printf("错误信息：%s \n",e.getMessage());
			System.exit(1);
		}
		
		
		try {
			long startTime = System.currentTimeMillis();
			DecimalFormat df2  = new DecimalFormat("##0.00");
			for (int i = 0; i < ddls.length; i++) {
				String sql = "drop table if exists "+ddls[i][0];
				pstmt = conn.prepareStatement(sql);
				pstmt.executeUpdate();
				
				String sql2 = "create "+(unlogged_tables ? " unlogged" : "")+" table  "+ddls[i][0]+"("+ddls[i][1]+")";
				sql2 += " with (fillfactor= "+fillfactor+")";	
				
				if (tablespace != null)
				{
					sql2 +=  " tablespace "+tablespace;
				}
				pstmt = conn.prepareStatement(sql2);
				pstmt.executeUpdate();
			}
			
			conn.setAutoCommit(false);
			
			for (int i = 0; i < nbranches * scale; i++){
				String sql3 = "insert into pgbench_branches(bid,bbalance) values("+(i + 1)+",0)";
				pstmt = conn.prepareStatement(sql3);
				pstmt.executeUpdate();
			}
			
			for (int i = 0; i < ntellers * scale; i++){
				String sql4 = "insert into pgbench_tellers(tid,bid,tbalance) values ("+(i + 1)+","+(i / ntellers + 1)+",0)";
				pstmt = conn.prepareStatement(sql4);
				pstmt.executeUpdate();
			}
			
			conn.commit();
			
			System.out.println("creating tables...");
			
			String sql5 = "truncate pgbench_accounts";
			pstmt = conn.prepareStatement(sql5);
			pstmt.executeUpdate();
			
			String sql6 = "";
			Statement stmt = conn.createStatement();
			for (long i = 0; i < naccounts * scale; i++) {
				long j = i+1;
				
				sql6 = "insert into pgbench_accounts(aid,bid,abalance) values("+(i+1)+",1,0)";
				stmt.addBatch(sql6);
				
				if((j  % (naccounts * scale /10)) == 0 || (j == naccounts * scale)){
					stmt.executeBatch();
				}
				
				long endTime = System.currentTimeMillis();
				double elapsed_sec = (endTime - startTime)/1000.00;
				double remaining_sec = ((double) scale * naccounts -  j) * elapsed_sec / j;
				
				if ((!use_quiet) && (j  % naccounts == 0)){
					System.out.println(j +" of "+(naccounts * scale) + "  tuples ("+((j  * 100)/(naccounts * scale)+"%)")+
							"   done (elapsed "+df2.format(elapsed_sec)+" s, remaining "+df2.format(remaining_sec)+" s)");	
				}
				else if(use_quiet && (j  % 100 == 0)){
					if ((j == scale * naccounts) || (elapsed_sec >= log_interval * LOG_STEP_SECONDS)){
							System.out.println(j +" of "+(naccounts * scale) + "  tuples ("+((j * 100)/(naccounts * scale)+"%)")+
									"   done (elapsed "+df2.format(elapsed_sec)+" s, remaining "+df2.format(remaining_sec)+" s)");	
						log_interval = (int) Math.ceil(elapsed_sec / LOG_STEP_SECONDS);
					}
				}
			}
			
			conn.commit();
			conn.setAutoCommit(true);
			/* vacuum */
			if (!is_no_vacuum){
				System.out.printf("vacuum...\n");
				pstmt = conn.prepareStatement("vacuum analyze pgbench_branches");
				pstmt.execute();
				pstmt = conn.prepareStatement("vacuum analyze pgbench_tellers");
				pstmt.execute();
				pstmt = conn.prepareStatement("vacuum analyze pgbench_accounts");
				pstmt.execute();
				pstmt = conn.prepareStatement("vacuum analyze pgbench_history");
				pstmt.execute();
			}
			System.out.println("set primary keys...");
			for (String ddlIndexe : ddlIndexes) {
				if (index_tablespace != null){
					ddlIndexe += " using index tablespace "+index_tablespace;
				}
				
				pstmt = conn.prepareStatement(ddlIndexe);
				pstmt.executeUpdate();
			}
			
			/*
			 * create foreign keys
			 */
			if (foreign_keys){
				System.out.println("set foreign keys...");
				for (String ddlKey : ddlKeys) {
					pstmt = conn.prepareStatement(ddlKey);
					pstmt.executeUpdate();
				}
			}
			System.out.println("done.");
			pstmt.close();
		} catch (SQLException e) {
			System.out.printf("错误信息：%s \n",e.getMessage());
			System.exit(1);
		}
			
	}
	
	public void usage(){
		System.out.println("bench is a benchmarking tool for hhdabase.\n\n"
			  +"Usage:\n"
			  + "  bench [OPTION]... [DBNAME]\n"
			  +   "\nInitialization options:\n"
			  +  "  -i         invokes initialization mode\n"
			  +   "  -F NUM     set fill factor\n"
			  +"  -n           do not run VACUUM after initialization\n"
			  +"  -q               quiet logging (one message each 5 seconds)\n"
			  + "  -s NUM          scaling factor\n"
			  + "  -fk           create foreign key constraints between tables\n"
			  + "  -it TABLESPACE \n"
			  +"                           create indexes in the specified tablespace\n"
			  + "  -tp TABLESPACE  create tables in the specified tablespace\n"
			  + "  -ut         create tables as unlogged tables\n"
			  + "\nOptions to select what to run:\n"
			  + "  -b NAME[@W]   add builtin script NAME weighted at W (default: 1)\n"
			  +"                           (use \"-b list\" to list available scripts)\n"
			  + "  -f FILENAME[@W]  add script FILENAME weighted at W (default: 1)\n"
			  + "  -N   skip updates of pgbench_tellers and pgbench_branches\n"
			  + "                           (same as \"-b simple-update\")\n"
			  + "  -S        perform SELECT-only transactions\n"
			  + "                           (same as \"-b select-only\")\n"
			  + "\nBenchmarking options:\n"
			  + "  -c NUM         number of concurrent database clients (default: 1)\n"
			  + "  -C             establish new connection for each transaction\n"
			  + "  -D  VALUE \n"
			  +"                           define variable for use by custom script\n"
			  +"  -j NUM           number of threads (default: 1)\n"
			  +"  -l              write transaction times to log file\n"
//			  +"  -L, --latency-limit=NUM  count transactions lasting more than NUM ms as late\n"
			  +"  -M, --protocol=simple|extended|prepared\n"
			  +"                           protocol for submitting queries (default: simple)\n"
			  +"  -n           do not run VACUUM before tests\n"
			  +"  -P  NUM       show thread progress report every NUM seconds\n"
			  +"  -r    report average latency per command\n"
			  +"  -R NUM           target rate in transactions per second\n"
			  +"  -s NUM          report this scale factor in output\n"
			  +"  -t NUM   number of transactions each client runs (default: 10)\n"
			  +"  -T NUM           duration of benchmark test in seconds\n"
			  + "  -v          vacuum all four standard tables before tests\n"
			  + "  --ai NUM 				aggregate data over NUM seconds\n"
//			  +"  --progress-timestamp     use Unix epoch timestamps for progress\n"
			  +"  --sr NUM      fraction of transactions to log (e.g., 0.01 for 1%%)\n"
			  +"\nCommon options:\n"
			  +"  -d debug              print debugging output\n"
			  +"  -h HOSTNAME      database server host or socket directory\n"
			  +"  -p PORT          database server port number\n"
			  +"  -U USERNAME  connect as specified database user\n"
			  +"  -V, --version            output version information, then exit\n"
			  +"  -?, --help               show this help, then exit\n"
			  +"\n"
			  +"Report bugs to <support@hhdatabase.com.cn>.\n"
			   );
	}
	
}
