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
import net.floodlightcontroller.util.FlowModUtils;
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
    private static int ELEPHANT_FLOW_BANDWIDTH = 3000;
    private static int ELEPHANT_FLOW_NUMBER = 1;
    private static long BLOCK_TIME_VALUE = 60;
    protected Map<IOFSwitch, Map<Long, OFPort>> macSwitchPortMap;
    protected Map<Long, Long> bListHosts;

    // for managing our map sizes
    protected static final int MAX_MACS_SWITCH = 100;
    public static final int E_SWITCH_APP_ID = 1;

    public static final int APP_ID_BITS = 12;
    public static final int APP_ID_SHIFT = (64 - APP_ID_BITS);
    public static final long ELEPHANT_SWITCH_COOKIE = (long) (E_SWITCH_APP_ID & ((1 << APP_ID_BITS) - 1)) << APP_ID_SHIFT;

    // more flow-mod defaults
    protected static final short IDLE_TIMEOUT_DEFAULT = 15;
    protected static final short HARD_TIMEOUT_DEFAULT = 5;
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
        macSwitchPortMap = new ConcurrentHashMap<>();
        bListHosts = new ConcurrentHashMap<>();

    }

    @Override
    public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
        floodlightProvider.addOFMessageListener(OFType.ERROR, this);
    }

    @Override
    public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {

        switch (msg.getType()) {
            case PACKET_IN:
                return this.processPacketInMessage(sw, (OFPacketIn) msg, cntx);
            case FLOW_REMOVED:
                return this.processFlowRemovedMessage(sw, (OFFlowRemoved) msg);
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
        Map<Long, OFPort> swMap = macSwitchPortMap.get(sw);

        logger.info("Got Mac address : {}", mac);
        logger.info("Got Port of source  : {}", portVal);

        if (swMap == null) {
            swMap = Collections.synchronizedMap(new LRULinkedHashMap<Long, OFPort>(MAX_MACS_SWITCH));
            macSwitchPortMap.put(sw, swMap);
        }
        swMap.put(mac,portVal);
    }

    /**
     * Removes a host from the MAC->SwitchPort mapping
     * @param sw The switch to remove the mapping from
     * @param mac The MAC address of the host to remove
     */
    protected void removeFromPortMap(IOFSwitch sw, long mac) {
        Map<Long, OFPort> swMap = macSwitchPortMap.get(sw);
        if (swMap != null)
            swMap.remove(mac);
    }

    public OFPort getFromPortMap(IOFSwitch sw, long mac) {
        Map<Long, OFPort> swMap = macSwitchPortMap.get(sw);
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
                              Match match, OFPort outPort, boolean ifDrop) {
        OFFlowMod.Builder flowMod;
        flowMod = sw.getOFFactory().buildFlowAdd();
        flowMod.setMatch(match);
        flowMod.setCookie(U64.of(ElephantControlSwitch.ELEPHANT_SWITCH_COOKIE));
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

        List<OFAction> al = new ArrayList<OFAction>();
        // No action to drop
        if(!ifDrop) {
            al.add(sw.getOFFactory().actions().buildOutput().setPort(outPort).setMaxLen(0xffFFffFF).build());
            flowMod.setActions(al);
        }
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

//      learn the port for this MAC
        this.addToPortMap(sw, sourceMac.getLong(), inPort);

        OFPort outPort = getFromPortMap(sw,  destMac.getLong());
        if(outPort != null)
            logger.info("Got output port as : {}", outPort.toString());

//        Filter-out hosts in blacklist
// Also, when the host is in blacklist check if the blockout time is expired and handle properly

        if (bListHosts.get(sourceMac.getLong())!= null){
            logger.info("Blacklisted host is {}", sourceMac.getLong());
            long blackListedHostTime = bListHosts.get(sourceMac.getLong());
            logger.info("Blacklisted host time remaining {}", blackListedHostTime);
            if(blackListedHostTime - HARD_TIMEOUT_DEFAULT == 0)
                bListHosts.remove(sourceMac.getLong());
            else {
                blackListedHostTime = blackListedHostTime - HARD_TIMEOUT_DEFAULT;
                bListHosts.put(sourceMac.getLong(), blackListedHostTime);
                logger.info("Blocking packets");
//                this.writePacketOutForPacketIn(sw, pi, OFPort.ZERO);
//                this.pushPacket(sw, m, pi, inPort);
//                this.writeFlowMod(sw, OFFlowModCommand.ADD, OFBufferId.NO_BUFFER, m, outPort, true);
                doDropFlow(sw, pi, m);
            }
            return Command.CONTINUE;
        }

        if (outPort == null) {
            // If we haven't learned the port for the dest MAC, flood it
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
//            this.writeFlowMod(sw, OFFlowMod.OFPFC_ADD, pi.getBufferId(), match, ????);
            logger.info("Sending packet to out port");
            this.pushPacket(sw, m, pi, outPort);
            this.writeFlowMod(sw, OFFlowModCommand.ADD, OFBufferId.NO_BUFFER, m, outPort, false);

        }

        return Command.CONTINUE;
    }

    /**
     * Processes a flow removed message.
     * @param sw The switch that sent the flow removed message.
     * @param flowRemovedMessage The flow removed message.
     * @return Whether to continue processing this message or stop.
     */
    private Command processFlowRemovedMessage(IOFSwitch sw, OFFlowRemoved flowRemovedMessage) {
        if (!flowRemovedMessage.getCookie().equals(U64.of(ElephantControlSwitch.ELEPHANT_SWITCH_COOKIE))) {
            return Command.CONTINUE;
        }
        Match match = flowRemovedMessage.getMatch();
        MacAddress sourceMac = match.get(MatchField.ETH_SRC);
        MacAddress destMac = match.get(MatchField.ETH_DST);

        logger.info("{} flow entry removed {}", sw, flowRemovedMessage);

        long bandwidth = (flowRemovedMessage.getDurationSec() != 0) ?flowRemovedMessage.getByteCount().getValue()/flowRemovedMessage.getDurationSec():0;
        logger.info("Bandwidth is {} ", bandwidth);
        logger.info("User Space: The flowRemovedMessage.getByteCount is {}", flowRemovedMessage.getByteCount().getValue());
        logger.info("User Space: The flowRemovedMessage.getDurationSeconds is {}", flowRemovedMessage.getDurationSec());

        if(bandwidth > ELEPHANT_FLOW_BANDWIDTH){
            logger.info("Adding {} to blacklist", sourceMac.getLong());
            bListHosts.put(sourceMac.getLong(), BLOCK_TIME_VALUE);
        }

        return Command.CONTINUE;
    }


    protected void doDropFlow(IOFSwitch sw, OFPacketIn pi, Match m) {
        OFPort inPort = OFMessageUtils.getInPort(pi);
//        Match m = createMatchFromPacket(sw, inPort, pi, cntx);
        OFFlowMod.Builder fmb = sw.getOFFactory().buildFlowAdd();
        List<OFAction> actions = new ArrayList<>(); // set no action to drop
       // List<OFAction> actions = new ArrayList<>();
//        U64 flowSetId = flowSetIdRegistry.generateFlowSetId();
//        U64 cookie = makeForwardingCookie(decision, flowSetId);

        /* If link goes down, we'll remember to remove this flow */
//        if (! m.isFullyWildcarded(MatchField.IN_PORT)) {
//            flowSetIdRegistry.registerFlowSetId(new NodePortTuple(sw.getId(), m.get(MatchField.IN_PORT)), flowSetId);
//        }

        logger.info("Dropping");
//        fmb.setCookie(cookie)
        fmb.setCookie(U64.of(ElephantControlSwitch.ELEPHANT_SWITCH_COOKIE))
                .setHardTimeout(HARD_TIMEOUT_DEFAULT)
                .setIdleTimeout(IDLE_TIMEOUT_DEFAULT)
                .setBufferId(OFBufferId.NO_BUFFER)
                .setMatch(m)
                .setPriority(PRIORITY_DEFAULT);

        FlowModUtils.setActions(fmb, actions, sw);

        /* Configure for particular switch pipeline */
//        if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_10) != 0) {
//            fmb.setTableId(FLOWMOD_DEFAULT_TABLE_ID);
//        }

        if (logger.isDebugEnabled()) {
            logger.debug("write drop flow-mod sw={} match={} flow-mod={}",
                    new Object[] { sw, m, fmb.build() });
        }
//        boolean dampened = messageDamper.write(sw, fmb.build());
        sw.write(fmb.build());
    }

}
