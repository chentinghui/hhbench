package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


import tool.ReadXML;

public class Operation extends Thread {

	private int runTimes = 10;
	private long costTime = 0;
	private long costAverageTime = 0;
	
	
	long progress_runNumber = 0;					//运行次数
	long progress_runTime = 0;						//运行时间
	long progress_transaction_number = 0;			//事务数量
	long progress_transaction_average = 0;			//事务average延迟
	long progress_transaction_stddev = 0;			//事务stddev延迟
	long agg_interval_transaction_number = 0;    	/* 临时事务个数 */
	
	public  Connection conn = null;
	RealizationOfParameters rop;
	int clients_id;									/* 客户id */
	int run_clients;								/* 客户个数 */
	int thread_id;									/* 线程id */
	
	long run_progressStartTime = System.currentTimeMillis();
	long run_progressTime = run_progressStartTime;
	public Operation() {}
	
	public Operation(RealizationOfParameters rop,int run_clients,int thread_id) {
		runTimes = rop.nxacts;
		this.run_clients = run_clients;
		this.thread_id = thread_id;
		this.rop = rop;
	}
	
	public void run() {
		PreparedStatement pstmt = null;
		run_progressTime = run_progressStartTime+rop.progress*1000;
		try {
			List<Integer> scripts_random = new ArrayList<Integer>();
			for (int i = 0; i < rop.num_scripts; i++) {
				for (int j = 0; j < Integer.parseInt(rop.sql_script[i][2])  ; j++) {
					scripts_random.add(i);
				}
			}
			
			if(rop.duration == 0){
				for (int k = 0; k < run_clients; k++) {
					clients_id = rop.clients_id++;
					for (int i = 0; i < runTimes; i++) {
						if(rop.throttle_delay > 0){
							sleep(rop.throttle_delay);
						}
						runDetail(scripts_random,pstmt,i);
					}
				}
			}else{
				long end_Time = System.currentTimeMillis()+1000 *rop.duration;
				
				for(int i = 0; end_Time >= System.currentTimeMillis();i++ ){
					if(rop.throttle_delay > 0){
						sleep(rop.throttle_delay);
					}
					runDetail(scripts_random,pstmt,i);
				}
			}
		} catch (SQLException e1) {
			System.out.printf("错误信息：%s \n",e1.getMessage());
			System.exit(1);
		} catch (Exception e1) {
			System.out.printf("错误信息：%s\n",e1.getMessage());
			System.exit(1);
		}
	}
	
	void runDetail(List<Integer> scripts_random,PreparedStatement pstmt,int i) throws SQLException,Exception{
		long startTime = System.currentTimeMillis();
		java.util.Random random =new java.util.Random();
		int result_num_scripts=scripts_random.get(random.nextInt(scripts_random.size()));
		
		if (conn == null || rop.is_connect) {
			 createConnection();
			 long conn_time = System.currentTimeMillis();
			 rop.CState[rop.num_scripts][3]	+= (conn_time - startTime);						//3:连接总花费时间
		}
		
		if (rop.debug){
			rop.logger.debug("client "+clients_id+" executing script \""+rop.sql_script[result_num_scripts][0]+"\"");
		}
		
		String[] sql = rop.sql_script[result_num_scripts][1].split("\n");
		for (int j = 0; j < sql.length; j++) {
			long sql_starttime = System.currentTimeMillis();
			pstmt = conn.prepareStatement(sql[j]);
			pstmt.execute();
			long sql_endtime = System.currentTimeMillis();
			rop.each_sql_total_time[result_num_scripts][j] += (sql_endtime -sql_starttime);
		}
		
		long endTime = System.currentTimeMillis();
		
		if (rop.debug){
			rop.logger.debug("client "+clients_id+" sending "+rop.sql_script[result_num_scripts][1]+"");
		}
		rop.scriptRuntims[result_num_scripts]++;
		
		rop.CState[result_num_scripts][0] += 	endTime - startTime;						//0:单独脚本运行事务的时间(average)
		rop.CState[result_num_scripts][1] += ((endTime - startTime)*(endTime - startTime));	//1:单独脚本运行事务平方时间(stddev)
		rop.CState[rop.num_scripts][2]	+=  endTime - startTime;							//2:脚本总花费时间
		rop.CState[rop.num_scripts][4]++;													//4:完成事务次数 
		rop.CState[rop.num_scripts][5] += (endTime - startTime)*(endTime - startTime);		//5:脚本总花费事务平方时间
		if(rop.CState[rop.num_scripts][4] == 1 || (endTime - startTime) < rop.CState[rop.num_scripts][6]){
			rop.CState[rop.num_scripts][6] =  endTime - startTime;							//6:事务的时间最小值
		}
		if(rop.CState[rop.num_scripts][4] == 1 || (endTime - startTime) > rop.CState[rop.num_scripts][7]){
			rop.CState[rop.num_scripts][7] =  endTime - startTime;							//7:事务的时间最大值
		}

		if(rop.progress > 0 && run_progressTime <= System.currentTimeMillis()){
			double tps = (new Float(rop.CState[rop.num_scripts][4] - progress_runNumber)/ (rop.CState[rop.num_scripts][2] - progress_runTime) * 1000.0D);
			double latency = (1.0*(rop.CState[result_num_scripts][0] - progress_transaction_average)/(rop.CState[rop.num_scripts][4]-progress_transaction_number));
			double stdev = (1.0*rop.CState[result_num_scripts][1]-progress_transaction_stddev)/(rop.CState[rop.num_scripts][4]-progress_transaction_number);
			System.out.printf("thread_id: %d ,progress: %s s, %.1f tps, lat %.3f ms stddev %.3f",thread_id,1.0*((System.currentTimeMillis()-run_progressStartTime)/1000), tps, latency, stdev);
			
			if(rop.throttle_delay == 0){
				System.out.println("\n");
			}
			
			run_progressTime = run_progressTime + rop.progress*1000;
			progress_transaction_average = rop.CState[result_num_scripts][0];
			progress_transaction_stddev = rop.CState[result_num_scripts][1];
			progress_transaction_number = rop.CState[rop.num_scripts][4];
			progress_runTime = rop.CState[rop.num_scripts][2];
			progress_runNumber = rop.CState[rop.num_scripts][4];
			
		}
		
		if ((rop.progress > 0 && run_progressTime <= System.currentTimeMillis()) && rop.throttle_delay > 0){
			System.out.printf(", lag %.3f ms \n", 1.0*rop.CState[rop.num_scripts][5]);	
//			if (rop.latency_limit)
//				fprintf(stderr, ", " INT64_FORMAT " skipped",
//						cur.skipped - last.skipped);
		}
		
		if (rop.debug){
			rop.logger.debug("client "+clients_id+" receiving");
		}
		if(rop.use_log){
			if (rop.agg_interval > 0){
					StringBuffer logfile = new StringBuffer();
					logfile.append(startTime+" "+(rop.CState[rop.num_scripts][4]-agg_interval_transaction_number));
					logfile.append(" "+rop.CState[result_num_scripts][0]+" "+rop.CState[result_num_scripts][1]);
					logfile.append(" "+rop.CState[rop.num_scripts][6]+" "+rop.CState[rop.num_scripts][7]);
					if(rop.throttle_delay > 0){}
					agg_interval_transaction_number = rop.CState[rop.num_scripts][4];
					writeLog(logfile.toString());

			}else if (rop.sample_rate != 0.0 && Math.random() <= rop.sample_rate){
				String logfile = clients_id+" "+i+" "+result_num_scripts+" "+(endTime/1000)+" "+(endTime - startTime);
				writeLog(logfile);
			}else if(rop.sample_rate == 0.0){
				String logfile = clients_id+" "+i+" "+result_num_scripts+" "+(endTime/1000)+" "+(endTime - startTime);
				writeLog(logfile);
			}
		}
		pstmt.close();
	}
	
	public   Connection  createConnection() throws Exception{
		try {
			Class.forName(ReadXML.getInstance().getDataBaseDriver());
			String url = ReadXML.getInstance().getUrl();
			String name = ReadXML.getInstance().getSqlName();
			String password = ReadXML.getInstance().getSqlPass();
			conn = DriverManager.getConnection(url, name, password);
			
			return conn;
		} catch (ClassNotFoundException e) {
			System.out.printf("错误信息：%s \n",e.getMessage());
			System.exit(1);
		} catch (SQLException e) {
			System.out.printf("错误信息：%s \n",e.getMessage());
			System.exit(1);
		}catch (Exception e) {
			System.out.printf("错误信息：%s \n",e.getMessage());
			System.exit(1);
		}
		return null;
	}
	
	
	

	public long getCostTime() {
		return this.costTime;
	}

	public long getAverageTime() {
		return this.costAverageTime;
	}
	
	public  Connection getConn() {
		return conn;
	}

	public  void setConn(Connection conn) {
		this.conn = conn;
	}
	
	public void setUrl(String host,String port,String database){
		String url = "jdbc:hhdbsql://"+host+":"+port+"/"+database;
		ReadXML.getInstance().setUrl(url);
	}
	
	public  void writeLog(String str){
		String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0]; 
		String path="hhbench.log."+pid+"."+(thread_id == 0 ? "": thread_id);
		
		File logFile=new File(path);
	      if(!logFile.exists()){
	    	  try {
				logFile.createNewFile();
			} catch (IOException e) {
				System.out.printf("错误信息：%s \n",e.getMessage());
				System.exit(1);
			}
	      }
	       
	    FileOutputStream out = null;
		try {
			//如果追加方式用true    
			out = new FileOutputStream(logFile,true); 
			StringBuffer sb=new StringBuffer();
			sb.append(str+"\n");
        	//注意需要转换对应的字符集
			out.write(sb.toString().getBytes("utf-8"));
			out.close();
		}catch (FileNotFoundException e) {
			System.out.printf("错误信息：%s \n",e.getMessage());
			System.exit(1);
		}catch (IOException e) {
			System.out.printf("错误信息：%s \n",e.getMessage());
			System.exit(1);
		}
       
	}
	
}
