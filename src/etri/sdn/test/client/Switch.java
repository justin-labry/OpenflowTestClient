package etri.sdn.test.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.openflow.io.OFMessageAsyncStream;
import org.projectfloodlight.openflow.protocol.OFCapabilities;
import org.projectfloodlight.openflow.protocol.OFDescStatsReply;
import org.projectfloodlight.openflow.protocol.OFFactories;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFeaturesReply;
import org.projectfloodlight.openflow.protocol.OFHelloElem;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketInReason;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFPortFeatures;
import org.projectfloodlight.openflow.protocol.OFPortState;
import org.projectfloodlight.openflow.protocol.OFStatsRequest;
import org.projectfloodlight.openflow.protocol.OFStatsType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import etri.sdn.controller.protocol.packet.Ethernet;
import etri.sdn.controller.protocol.packet.IPv4;
import etri.sdn.controller.protocol.packet.TCP;
import etri.sdn.controller.util.StackTrace;

public class Switch implements Runnable  {

	static Logger logger = LoggerFactory.getLogger(Switch.class);

	private enum STATUS { S, R };

	private SocketChannel socket;
	private OFMessageAsyncStream stream;
	private volatile boolean doLoop = true;

	private STATUS status = STATUS.S;
	private long receivedMsgCount = 0;
	
	private int macAddresses;
	private int load;
	private long datapathId;
	private String controllerAddress;
	private int controllerPort;
	
	public Switch(String controllerAddress, int controllerPort,
			int macAddresses, int load, long datapathId) {
		this.controllerAddress = controllerAddress;
		this.controllerPort = controllerPort;
		this.macAddresses = macAddresses;
		this.load = load;
		this.datapathId = datapathId;
	}

	public void shutdown() {
		this.doLoop = false;
	}

	private OFMessage processMsg(OFFactory fac, OFMessage m) {
		this.receivedMsgCount += 1;
		
		switch ( m.getType() ) {	
		case FEATURES_REQUEST:
			// we should send a FEATURES_REPLY:
			OFPortDesc p1 = 
			fac.buildPortDesc()
			.setPortNo(OFPort.of(1))
			.setAdvertised(EnumSet.of(OFPortFeatures.PF_1GB_FD))
			.setCurr(EnumSet.of(OFPortFeatures.PF_1GB_FD))
			.setName("eth1")
			.setHwAddr(MacAddress.of("00:00:00:00:00:01"))
			.setPeer(EnumSet.of(OFPortFeatures.PF_1GB_FD))
			.setState(EnumSet.of(OFPortState.STP_FORWARD, OFPortState.STP_LISTEN))
			.setSupported(EnumSet.of(OFPortFeatures.PF_1GB_FD)).build();

			OFPortDesc p2 = 
					p1.createBuilder()
					.setPortNo(OFPort.of(2))
					.setName("eth2")
					.setHwAddr(MacAddress.of("00:00:00:00:00:02"))
					.build();

			OFFeaturesReply.Builder fr = fac.buildFeaturesReply();
			fr
			.setDatapathId(DatapathId.of(this.datapathId))
			.setNTables((short)1)
			.setNBuffers((short)255)
			.setCapabilities(EnumSet.of(OFCapabilities.PORT_STATS, OFCapabilities.TABLE_STATS, OFCapabilities.FLOW_STATS))
			.setPorts( Arrays.<OFPortDesc>asList( p1, p2 ));

			this.status = STATUS.R;

			return fr.build();
			
		case STATS_REQUEST:
			OFStatsRequest<?> sr = (OFStatsRequest<?>) m;
			if ( sr.getStatsType() == OFStatsType.DESC ) {
				OFDescStatsReply.Builder ds = fac.buildDescStatsReply();
				ds
				.setSwDesc("bjlee").setSerialNum("1.0.0").setMfrDesc("bjlee")
				.setHwDesc("bjlee-sw").setDpDesc("" + this.datapathId);
				return ds.build();
			}
			break;

		default:
			break;
		}

		return null;
	}

	@Override
	public void run() {
		// open a client connection
		try {
			this.socket = SocketChannel.open();
			this.socket.connect( new InetSocketAddress(this.controllerAddress, this.controllerPort) );
			this.socket.configureBlocking(false);
		} catch ( IOException e ) {
			logger.error("cannot open client socket to controller: {}", StackTrace.of(e));
			return;
		}

		try {
			this.socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			this.socket.setOption(StandardSocketOptions.TCP_NODELAY, true);
		} catch (IOException e) {
			logger.error("cannot open client socket to controller: {}", StackTrace.of(e));
			return;
		}

		// create stream
		try {
			this.stream = new OFMessageAsyncStream( this.socket );
		} catch (IOException e) {
			logger.error("cannot create stream: {}", StackTrace.of(e));
			return;
		}

		// create message factory (1.0)
		OFFactory fac = OFFactories.getFactory(OFVersion.OF_10);

		Queue<OFMessage> queue = new ConcurrentLinkedQueue<>();
		queue.add( fac.hello(Collections.<OFHelloElem>emptyList()) );
		
		// before starting the test, we create a source and destination mac pairs. 
		MacAddress[] sources = new MacAddress[this.macAddresses];
		MacAddress[] destinations = new MacAddress[this.macAddresses];
		
		for ( int i = 0; i < this.macAddresses * 2; ++i ) {
			if ( i % 2 == 0 ) {
				sources[i / 2] = MacAddress.of( (this.datapathId-1)*this.macAddresses*2 + i + 10);
			} else {
				destinations[i / 2] = MacAddress.of( (this.datapathId-1)*this.macAddresses*2 + i + 10);
			}
		}
		
		Random random = new Random();
		
//		logger.info("sources = {}", Arrays.toString( sources ) );
//		logger.info("destinations = {}", Arrays.toString( destinations ) );

		// now, we start a selection loop.
		try {
			// open selector.
			Selector selector = Selector.open();
			// register the socket to read and write event.
			this.socket.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, null);

			int bufferId = 0;

			while ( doLoop ) {
				int r = selector.select(100);
				if ( r > 0 ) {
					Set<SelectionKey> keys = selector.selectedKeys();
					for ( Iterator<SelectionKey> i = keys.iterator(); i.hasNext(); ) {
						SelectionKey key = i.next();
						i.remove();

						if ( key.isValid() ) {
							// if key is readable
							if ( key.isReadable() ) {
								List<OFMessage> msgs = this.stream.read();
								if ( msgs != null ) {
									for ( OFMessage m : msgs ) {
										OFMessage response = processMsg( fac, m );
										if ( response != null ) {
											queue.add( response );
										}
									}
								}
							}

							// if key is writable
							if ( key.isWritable() ) {

								if ( queue.size() < this.load && this.status == STATUS.R ) {
									// we create a new packet-in msg and put it into the queue.

									++bufferId;

									// first, create two mac address one is on the port one and 
									// second is on the port two. 
									
									MacAddress onOne = sources[ random.nextInt( sources.length ) ];
									MacAddress onTwo = destinations [ random.nextInt( destinations.length ) ];
									EthType ethType = EthType.IPv4;

									// if buffer id is even number, then 1 is chosen as input port.
									// Otherwise, 2 is chosen as input port. 
									OFPort inPort = OFPort.of( bufferId % 2 + 1 );
									MacAddress srcAddr = ( inPort.getPortNumber() == 1 ) ? onOne : onTwo ;
									MacAddress dstAddr = ( inPort.getPortNumber() == 1 ) ? onTwo : onOne ;

									Match.Builder match = fac.buildMatch();
									match.setExact(MatchField.ETH_TYPE, ethType);
									match.setExact(MatchField.ETH_SRC, srcAddr);
									match.setExact(MatchField.ETH_DST, dstAddr);
									
									TCP tcp = new TCP();
									tcp.setSourcePort( (short) srcAddr.getLong() );
									tcp.setDestinationPort( (short) dstAddr.getLong() );

									IPv4 ip = new IPv4();
									ip.setProtocol(IPv4.PROTOCOL_TCP);
									ip.setTtl((byte) 64);
									ip.setDestinationAddress( (int) dstAddr.getLong() );
									ip.setSourceAddress( (int) srcAddr.getLong() );
									ip.setPayload( tcp );

									Ethernet eth = new Ethernet();
									eth.setEtherType( (short) ethType.getValue() );
									eth.setSourceMACAddress( srcAddr.getBytes() );
									eth.setDestinationMACAddress( dstAddr.getBytes() );
									eth.setPayload(ip);

									OFPacketIn.Builder pi = fac.buildPacketIn();
									pi
									.setBufferId(OFBufferId.of(bufferId))
									.setInPort( inPort )
									.setReason(OFPacketInReason.NO_MATCH)
									.setData( eth.serialize() );

									queue.add( pi.build() );
								}
								OFMessage msg = null;
								while ( (msg = queue.poll()) != null ) {
									this.stream.write( msg );
								} 
								this.stream.flush();
							}
						}
					}
				}
			}

		} catch (IOException e) {
			logger.error("exception: {}", StackTrace.of(e));
			return;
		}
	}

	public long getReceivedMessageCount() {
		return this.receivedMsgCount;
	}

	public long getDatapathId() {
		return this.datapathId;
	}
}
