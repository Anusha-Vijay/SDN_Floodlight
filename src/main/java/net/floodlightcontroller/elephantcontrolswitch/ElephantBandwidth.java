package net.floodlightcontroller.elephantcontrolswitch;

import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.resource.Post;
import org.slf4j.Logger;

/**
 * Created by Anushavijay on 12/8/16.
 */
public class ElephantBandwidth extends ElephantResourceBase{
    protected static Logger logger;

    @Post
    public Object store(String fmJson) {
        IElephantService elephantService = getElephantService();
        System.out.printf("Received json string is:"+fmJson);
        JSONObject bandwidthRequest=new JSONObject(fmJson);
        String bandwidth_s=bandwidthRequest.getString("bandwidth");
        System.out.printf("\nBandwidth requested is"+ bandwidth_s+"\n");
        int bandwidth=Integer.parseInt(bandwidth_s);
        System.out.println(bandwidth);
        System.out.printf("\n");
        //elephantService.setBandwidth(bandwidth);
        ElephantControlSwitch.ELEPHANT_FLOW_BANDWIDTH=bandwidth;
        setStatus(Status.SUCCESS_OK);
        System.out.println("The new elephantflow bandwidth is:"+ ElephantControlSwitch.ELEPHANT_FLOW_BANDWIDTH);
        return "{\"status\" : \"SUCCESS\", \"Details\": \"Elephant Bandwidth Control set to "+bandwidth_s+"\"}";


    }




}
