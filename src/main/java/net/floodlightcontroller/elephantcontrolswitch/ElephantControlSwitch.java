package net.floodlightcontroller.elephantcontrolswitch;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.util.OFMessageUtils;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.*;
import org.projectfloodlight.openflow.util.LRULinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by Anushavijay on 11/19/16.
 */

public class ElephantControlSwitch implements IFloodlightModule, IOFMessageListener {
    protected IFloodlightProviderService floodlightProvider;
    protected Set<Long> macAddresses;
    protected static Logger logger;
    private static int ELEPHANT_FLOW_BANDWIDTH = 500;
    private static int ELEPHANT_FLOW_NUMBER = 1;
    private static int BLOCK_TIME_VALUE = 10000;
    protected Map<IOFSwitch, Map<Long, OFPort>> macToSwitchPortMap;
    protected Map<Long, Long> blackListesHosts;

    // for managing our map sizes
    protected static final int MAX_MACS_PER_SWITCH  = 1000;

    // flow-mod - for use in the cookie
    public static final int ELEPHANT_SWITCH_APP_ID = 1;
    // LOOK! This should probably go in some class that encapsulates
    // the app cookie management
    public static final int APP_ID_BITS = 12;
    public static final int APP_ID_SHIFT = (64 - APP_ID_BITS);
    public static final long ELEPHANT_SWITCH_COOKIE = (long) (ELEPHANT_SWITCH_APP_ID & ((1 << APP_ID_BITS) - 1)) << APP_ID_SHIFT;

    // more flow-mod defaults
    protected static final short IDLE_TIMEOUT_DEFAULT = 10;
    protected static final short HARD_TIMEOUT_DEFAULT = 0;
    protected static final short PRIORITY_DEFAULT = 100;

    @Override
    public String getName() {
        return ElephantControlSwitch.class.getSimpleName();
    }

    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name) {
        return false;
    }

    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name) {
        return false;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        Collection<Class<? extends IFloodlightService>> l = new ArrayList<>();
        return l;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<>();
        return m;

    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        Collection<Class<? extends IFloodlightService>> l =
                new ArrayList<>();
        l.add(IFloodlightProviderService.class);
        return l;
    }

    @Override
    public void init(FloodlightModuleContext context) throws FloodlightModuleException {
        floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
        macAddresses = new ConcurrentSkipListSet<>();
        logger = LoggerFactory.getLogger(ElephantControlSwitch.class);
        macToSwitchPortMap = new ConcurrentHashMap<>();
        blackListesHosts = new ConcurrentHashMap<>();

    }

    @Override
    public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        floodlightProvider.addOFMessageListener(OFType.ERROR, this);
    }

    @Override
    public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
//        Ethernet eth =
//                IFloodlightProviderService.bcStore.get(cntx,
//                        IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
//
//        Long sourceMACHash = eth.getSourceMACAddress().getLong();
//        if (!macAddresses.contains(sourceMACHash)) {
//            macAddresses.add(sourceMACHash);
//            logger.info("MAC Address: {} seen on switch: {}",
//                    eth.getSourceMACAddress().toString(),
//                    sw.getId().toString());
//        }
//        return Command.CONTINUE;

        switch (msg.getType()) {
            case PACKET_IN:
                return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);
            case FLOW_REMOVED:
//                return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);
                logger.info("Got flow removed");
                return Command.CONTINUE;
            case ERROR:
                logger.info("received an error {} from switch {}", (OFErrorMsg) msg, sw);
                return Command.CONTINUE;
            default:
                break;
        }
        logger.error("received an unexpected message {} from switch {}", msg, sw);
        return Command.CONTINUE;
    }


    /**
     * Adds a host to the MAC->SwitchPort mapping
     * @param sw The switch to add the mapping to
     * @param mac The MAC address of the host to add
     * @param portVal The switchport that the host is on
     */
    protected void addToPortMap(IOFSwitch sw, long mac, OFPort portVal) {
        Map<Long, OFPort> swMap = macToSwitchPortMap.get(sw);

        logger.info("Got Mac address : {}", mac);
        logger.info("Got Port of source  : {}", portVal);

        if (swMap == null) {
            // May be accessed by REST API so we need to make it thread safe
            swMap = Collections.synchronizedMap(new LRULinkedHashMap<Long, OFPort>(MAX_MACS_PER_SWITCH));
            macToSwitchPortMap.put(sw, swMap);
        }
        swMap.put(mac,portVal);
    }

    /**
     * Removes a host from the MAC->SwitchPort mapping
     * @param sw The switch to remove the mapping from
     * @param mac The MAC address of the host to remove
     */
    protected void removeFromPortMap(IOFSwitch sw, long mac) {
        Map<Long, OFPort> swMap = macToSwitchPortMap.get(sw);
        if (swMap != null)
            swMap.remove(mac);
    }

    public OFPort getFromPortMap(IOFSwitch sw, long mac) {
        Map<Long, OFPort> swMap = macToSwitchPortMap.get(sw);
        if (swMap != null)
            return swMap.get(mac);
        else
            return null;
    }


    /**
     * Writes a OFFlowMod to a switch.
     * @param sw The switch tow rite the flowmod to.
     * @param command The FlowMod actions (add, delete, etc).
     * @param bufferId The buffer ID if the switch has buffered the packet.
     * @param match The OFMatch structure to write.
     * @param outPort The switch port to output it to.
     */
    private void writeFlowMod(IOFSwitch sw, OFFlowModCommand command, OFBufferId bufferId,
                              Match match, OFPort outPort) {
        // from openflow 1.0 spec - need to set these on a struct ofp_flow_mod:
        // struct ofp_flow_mod {
        //    struct ofp_header header;
        //    struct ofp_match match; /* Fields to match */
        //    uint64_t cookie; /* Opaque controller-issued identifier. */
        //
        //    /* Flow actions. */
        //    uint16_t command; /* One of OFPFC_*. */
        //    uint16_t idle_timeout; /* Idle time before discarding (seconds). */
        //    uint16_t hard_timeout; /* Max time before discarding (seconds). */
        //    uint16_t priority; /* Priority level of flow entry. */
        //    uint32_t buffer_id; /* Buffered packet to apply to (or -1).
        //                           Not meaningful for OFPFC_DELETE*. */
        //    uint16_t out_port; /* For OFPFC_DELETE* commands, require
        //                          matching entries to include this as an
        //                          output port. A value of OFPP_NONE
        //                          indicates no restriction. */
        //    uint16_t flags; /* One of OFPFF_*. */
        //    struct ofp_action_header actions[0]; /* The action length is inferred
        //                                            from the length field in the
        //                                            header. */
        //    };
        OFFlowMod.Builder flowMod;
        flowMod = sw.getOFFactory().buildFlowAdd();
//        if (command == OFFlowModCommand.DELETE) {
//            fmb = sw.getOFFactory().buildFlowDelete();
//        } else {
//            fmb = sw.getOFFactory().buildFlowAdd();
//        }
//        OFFlowMod flowMod = (OFFlowMod) floodlightProvider.get().getMessage(OFType.FLOW_MOD);
        flowMod.setMatch(match);
        flowMod.setCookie(U64.of(ElephantControlSwitch.ELEPHANT_SWITCH_COOKIE));
//        flowMod.setCommand(command);
        flowMod.setIdleTimeout(ElephantControlSwitch.IDLE_TIMEOUT_DEFAULT);
        flowMod.setHardTimeout(ElephantControlSwitch.HARD_TIMEOUT_DEFAULT);
        flowMod.setPriority(ElephantControlSwitch.PRIORITY_DEFAULT);
        flowMod.setBufferId(bufferId);
        flowMod.setOutPort((command == OFFlowModCommand.DELETE) ? OFPort.ANY : outPort);
        Set<OFFlowModFlags> sfmf = new HashSet<OFFlowModFlags>();
        if (command != OFFlowModCommand.DELETE) {
            sfmf.add(OFFlowModFlags.SEND_FLOW_REM);
        }
        flowMod.setFlags(sfmf);

        // set the ofp_action_header/out actions:
        // from the openflow 1.0 spec: need to set these on a struct ofp_action_output:
        // uint16_t type; /* OFPAT_OUTPUT. */
        // uint16_t len; /* Length is 8. */
        // uint16_t port; /* Output port. */
        // uint16_t max_len; /* Max length to send to controller. */
        // type/len are set because it is OFActionOutput,
        // and port, max_len are arguments to this constructor
        List<OFAction> al = new ArrayList<OFAction>();
        al.add(sw.getOFFactory().actions().buildOutput().setPort(outPort).setMaxLen(0xffFFffFF).build());
        flowMod.setActions(al);

        if (logger.isTraceEnabled()) {
            logger.trace("{} {} flow mod {}",
                    new Object[]{ sw, (command == OFFlowModCommand.DELETE) ? "deleting" : "adding", flowMod.build() });
        }

//        counterFlowMod.increment();

        // and write it out
        sw.write(flowMod.build());
    }

    /**
     * Writes an OFPacketOut message to a switch.
     * @param sw The switch to write the PacketOut to.
     * @param packetInMessage The corresponding PacketIn.
     * @param egressPort The switchport to output the PacketOut.
     */
    private void writePacketOutForPacketIn(IOFSwitch sw,
                                           OFPacketIn packetInMessage,
                                           OFPort egressPort) {
        OFMessageUtils.writePacketOutForPacketIn(sw, packetInMessage, egressPort);
//        counterPacketOut.increment();
    }

    /**
     * Pushes a packet-out to a switch.  The assumption here is that
     * the packet-in was also generated from the same switch.  Thus, if the input
     * port of the packet-in and the outport are the same, the function will not
     * push the packet-out.
     * @param sw        switch that generated the packet-in, and from which packet-out is sent
     * @param match     OFmatch
     * @param pi        packet-in
     * @param outport   output port
     */
    private void pushPacket(IOFSwitch sw, Match match, OFPacketIn pi, OFPort outport) {
        if (pi == null) {
            return;
        }

        OFPort inPort = (pi.getVersion().compareTo(OFVersion.OF_12) < 0 ? pi.getInPort() : pi.getMatch().get(MatchField.IN_PORT));

        // The assumption here is (sw) is the switch that generated the
        // packet-in. If the input port is the same as output port, then
        // the packet-out should be ignored.
        if (inPort.equals(outport)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Attempting to do packet-out to the same " +
                                "interface as packet-in. Dropping packet. " +
                                " SrcSwitch={}, match = {}, pi={}",
                        new Object[]{sw, match, pi});
                return;
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("PacketOut srcSwitch={} match={} pi={}",
                    new Object[] {sw, match, pi});
        }

        OFPacketOut.Builder pob = sw.getOFFactory().buildPacketOut();

        // set actions
        List<OFAction> actions = new ArrayList<OFAction>();
        actions.add(sw.getOFFactory().actions().buildOutput().setPort(outport).setMaxLen(0xffFFffFF).build());

        pob.setActions(actions);

        // If the switch doens't support buffering set the buffer id to be none
        // otherwise it'll be the the buffer id of the PacketIn
        if (sw.getBuffers() == 0) {
            // We set the PI buffer id here so we don't have to check again below
            pi = pi.createBuilder().setBufferId(OFBufferId.NO_BUFFER).build();
            pob.setBufferId(OFBufferId.NO_BUFFER);
        } else {
            pob.setBufferId(pi.getBufferId());
        }

        pob.setInPort(inPort);

        // If the buffer id is none or the switch doesn's support buffering
        // we send the data with the packet out
        if (pi.getBufferId() == OFBufferId.NO_BUFFER) {
            byte[] packetData = pi.getData();
            pob.setData(packetData);
        }

//        counterPacketOut.increment();
        sw.write(pob.build());
    }


    protected Match createMatchFromPacket(IOFSwitch sw, OFPort inPort, FloodlightContext cntx) {
        // The packet in match will only contain the port number.
        // We need to add in specifics for the hosts we're routing between.
        Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
        VlanVid vlan = VlanVid.ofVlan(eth.getVlanID());
        MacAddress srcMac = eth.getSourceMACAddress();
        MacAddress dstMac = eth.getDestinationMACAddress();

        Match.Builder mb = sw.getOFFactory().buildMatch();
        mb.setExact(MatchField.IN_PORT, inPort)
                .setExact(MatchField.ETH_SRC, srcMac)
                .setExact(MatchField.ETH_DST, dstMac);

        if (!vlan.equals(VlanVid.ZERO)) {
            mb.setExact(MatchField.VLAN_VID, OFVlanVidMatch.ofVlanVid(vlan));
        }

        return mb.build();
    }


    private Command processPacketInMessage(IOFSwitch sw, OFPacketIn pi, FloodlightContext cntx) {

        OFPort inPort = (pi.getVersion().compareTo(OFVersion.OF_12) < 0 ? pi.getInPort() : pi.getMatch().get(MatchField.IN_PORT));

        /* Read packet header attributes into Match */
        Match m = createMatchFromPacket(sw, inPort, cntx);
        MacAddress sourceMac = m.get(MatchField.ETH_SRC);
        MacAddress destMac = m.get(MatchField.ETH_DST);
//        VlanVid vlan = m.get(MatchField.VLAN_VID) == null ? VlanVid.ZERO : m.get(MatchField.VLAN_VID).getVlanVid();

//         CS6998: Do works here to learn the port for this MAC
        this.addToPortMap(sw, sourceMac.getLong(), inPort);


        /* CS6998: Do works here to implement super firewall
        Hint: You may check connection limitation here.

        /* CS6998: Filter-out hosts in blacklist
         *         Also, when the host is in blacklist check if the blockout time is
         *         expired and handle properly
           if (....)
            return Command.CONTINUE;
         */

        /* CS6998: Ask the switch to flood the packet to all of its ports
         *         Thus, this module currently works as a dummy hub
         */

//        this.writePacketOutForPacketIn(sw, pi, OFPort.OFPP_FLOOD.getValue());
//        this.writePacketOutForPacketIn(sw, pi, OFPort.FLOOD);

        // CS6998: Ask the switch to flood the packet to all of its ports
        // Now output flow-mod and/or packet
        // CS6998: Fill out the following ???? to obtain outPort
        OFPort outPort = getFromPortMap(sw,  destMac.getLong());
        if(outPort != null)
            logger.info("Got output port as : {}", outPort.toString());

        if (outPort == null) {
            // If we haven't learned the port for the dest MAC, flood it
            // CS6998: Fill out the following ????
            //this.writePacketOutForPacketIn(sw, pi, OFPort.OFPP_????.getValue());
            logger.info("output port null");
            this.writePacketOutForPacketIn(sw, pi, OFPort.FLOOD);
        } else if (outPort.equals(inPort)) {
            logger.trace("ignoring packet that arrived on same port as learned destination:"
                    + " switch {} dest MAC {} port {}",
                    new Object[]{ sw, destMac.toString(), outPort });
        } else {
            // Add flow table entry matching source MAC, dest MAC and input port
            // that sends to the port we previously learned for the dest MAC.
//            m.setWildcards(((Integer)sw.getAttribute(IOFSwitch.PROP_FASTWILDCARDS)).intValue()
//                    & ~OFMatch.OFPFW_IN_PORT
//                    & ~OFMatch.OFPFW_DL_SRC & ~OFMatch.OFPFW_DL_DST
//                    & ~OFMatch.OFPFW_NW_SRC_MASK & ~OFMatch.OFPFW_NW_DST_MASK);
            // CS6998: Fill out the following ????
//            this.writeFlowMod(sw, OFFlowMod.OFPFC_ADD, pi.getBufferId(), match, ????);
            logger.info("Sending packet to out port");
            this.pushPacket(sw, m, pi, outPort);
            this.writeFlowMod(sw, OFFlowModCommand.ADD, OFBufferId.NO_BUFFER, m, outPort);

        }

        return Command.CONTINUE;
    }
}
