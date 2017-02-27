package net.floodlightcontroller.elephantcontrolswitch;

import net.floodlightcontroller.core.module.IFloodlightService;


/**
 * Created by Anushavijay on 12/8/16.
 */
public interface IElephantService extends IFloodlightService {

    public int setBandwidth(int bandwidth);

    public void getBandwidth();
}
