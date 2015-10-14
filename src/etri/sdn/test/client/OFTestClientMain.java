package etri.sdn.test.client;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class OFTestClientMain {
	
	public static void main(String[] args) {
		
		Options options = new Options();
		options.addOption("c", "controller", true, "controller ip address");
		options.addOption("p", "port", true, "controller port number");
		options.addOption("m", "mac", true, "number of mac address / 2 per switch");
		options.addOption("l", "load", true, "message generating load");
		options.addOption("s", "switches", true, "number of switches (max 10)");
		
		// default values
		String controller = "127.0.0.1";
		int port = 6633;
		int mac = 200;
		int load = 10;
		int switches = 2;
		
		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse( options, args );
			if ( cmd.hasOption("c") ) {
				controller = cmd.getOptionValue("c");
			}
			if ( cmd.hasOption("p") ) {
				port = Integer.parseInt( cmd.getOptionValue("p") );
			}
			if ( cmd.hasOption("m") ) {
				mac = Integer.parseInt( cmd.getOptionValue("m") );
			}
			if ( cmd.hasOption("l") ) {
				load = Integer.parseInt( cmd.getOptionValue("l") );
			}
			if ( cmd.hasOption("s") ) {
				switches = Integer.parseInt( cmd.getOptionValue("s") );
				if ( switches > 10 ) {
					System.out.println( options.toString() );
					return;
				}
			}
		} catch (ParseException e1) {
			System.out.println( options.toString() );
			return;
		}
		
		final ExecutorService es = Executors.newFixedThreadPool(10);
		
		// wait for 100 seconds to terminate.
		try {
			System.out.println("Starting test...");
			
			List<Switch> initiated = new LinkedList<>();
			
			for ( int i = 0; i < switches; ++i ) {
				Switch s = new Switch(controller, port, mac, load, i+1);
				es.execute(s);
				initiated.add( s );
			}
	
			es.awaitTermination(100, TimeUnit.SECONDS);
			
			System.out.println("Shutting down the test...");
			for ( Switch s : initiated ) {
				s.shutdown();
			}
			
			for ( Switch s : initiated ) {
				System.out.println("" + s.getDatapathId() + ": " + "Total " + s.getReceivedMessageCount() + " messages are received.");
			}
			System.exit(0);
		} catch (InterruptedException e) {
			// execution interrupted.
		}
	}
}
