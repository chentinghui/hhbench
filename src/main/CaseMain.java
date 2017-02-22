package main;


import org.apache.log4j.Logger;


public class CaseMain {
	public static Logger logger = Logger.getLogger(""); 
	public static void main(String[] args) {
		RealizationOfParameters rop = new RealizationOfParameters(args,logger);
		if(!rop.is_init_mode){
			printResults(rop);
		}
	}
	
	public static void printResults(RealizationOfParameters rop){
		
			int nclients_dealt = 0;
			int run_clients = 0;
			
			Operation[] operators = new Operation[rop.nthreads];
			
			for (int i = 0; i < operators.length; i++) {
				run_clients = (rop.nclients - nclients_dealt + rop.nthreads - i - 1) / (rop.nthreads - i);
				nclients_dealt += run_clients;
				operators[i] = new Operation(rop,run_clients,i);
			}
			

			long startTime = System.currentTimeMillis();
			
			for (Operation operator : operators) {
				operator.start();
			}
	
			for (Operation operator : operators) {
				try {
					operator.join();
				} catch (InterruptedException e) {
					System.out.printf("错误信息：%s \n",e.getMessage());
					System.exit(1);
				}
			}
			long endTime = System.currentTimeMillis();
			double time_include = endTime-startTime;
		
			System.out.println("transaction type: "+((rop.num_scripts == 1) ? rop.sql_script[0][0] : "multiple scripts"));
			System.out.println("scaling factor: "+rop.scale);
			System.out.println("query mode: "+rop.querymode);
			System.out.println("number of clients: "+rop.nclients);
			System.out.println("number of threads: "+rop.nthreads);
			if (rop.duration <= 0){
				System.out.println("number of transactions per client: "+rop.nxacts);
				System.out.println("number of transactions actually processed: "+rop.CState[rop.num_scripts][4]+"/"+rop.nxacts*rop.nclients);
			}else{
				System.out.printf("duration: %d s\n", rop.duration);
				System.out.printf("number of transactions actually processed: " +rop.CState[rop.num_scripts][4]+ "\n");
			}
			if (rop.throttle_delay > 0 || rop.progress > 0 || rop.latency_limit){
				double	latency = rop.CState[rop.num_scripts][2]/new Float(rop.CState[rop.num_scripts][4]);
				double	stddev = Math.sqrt(rop.CState[rop.num_scripts][5] / rop.CState[rop.num_scripts][4] - latency * latency);
				System.out.printf("latency average = %.3f ms \n",latency);
				System.out.printf("latency stddev = %.3f ms \n", stddev);
			}else{
				System.out.printf ("latency average = %.3f ms \n",rop.CState[rop.num_scripts][2]/new Float(rop.CState[rop.num_scripts][4]));
			}
			
			if (rop.throttle_delay > 0){
				System.out.printf("rate limit schedule lag: avg %.3f (max %.3f) ms\n",
						 0.001 * rop.CState[rop.num_scripts][6], 0.001 * rop.CState[rop.num_scripts][7]);
			}
			
			System.out.println("tps = "+(new Float(rop.CState[rop.num_scripts][4])/ time_include * 1000.0D)+" (including connections establishing)");
			System.out.println("tps = "+(new Float(rop.CState[rop.num_scripts][4])/ (time_include - (rop.CState[rop.num_scripts][3]/rop.nclients)) * 1000.0D)+" (excluding connections establishing)");
		
			if (rop.per_script_stats || rop.latency_limit || rop.is_latencies){
				int	i;

				for (i = 0; i < rop.num_scripts; i++){
					if (rop.num_scripts > 1)
						System.out.printf("SQL script %d: %s\n"+
							   " - weight: %s (targets %.1f%% of total)\n"+
							   " - " +rop.scriptRuntims[i]+ " transactions (%.1f%% of total, tps = %f)\n",
							   i + 1, rop.sql_script[i][0],
							   rop.sql_script[i][2],
							   100.0 * Integer.parseInt(rop.sql_script[i][2]) / rop.total_weight,
							   new Float(rop.scriptRuntims[i] / rop.nclients*rop.nxacts),
							   rop.scriptRuntims[i] / time_include*1000D);
					else
						System.out.printf("script statistics:\n");


					if (rop.num_scripts > 1){
						double latency = 0.0;
						double stddev = 0.0;
						if(rop.scriptRuntims[i] != 0){
							latency = rop.CState[i][0] / rop.scriptRuntims[i];
							stddev = Math.sqrt(rop.CState[i][1] / rop.scriptRuntims[i] - latency * latency);
						}
						System.out.printf("- latency average = %.3f ms\n",	latency);
						System.out.printf("- latency stddev = %.3f ms\n",  stddev);
					}
			
					/* Report per-command latencies */
					if (rop.is_latencies){
						System.out.printf(" - statement latencies in milliseconds:\n");
						String[] sql = rop.sql_script[i][1].split("\n");
						for (int j = 0; j < sql.length; j++) {
							System.out.printf(" %11.3f ms %s\n",new Float(rop.each_sql_total_time[i][j]) /rop.scriptRuntims[i],
									sql[j]);
						}			
					}
				}
			}
			
			
		}
	
}
