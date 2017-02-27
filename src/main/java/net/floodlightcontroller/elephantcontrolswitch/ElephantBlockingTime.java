package net.floodlightcontroller.elephantcontrolswitch;

import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

/**
 * Created by Anushavijay on 12/8/16.
 */
public class ElephantBlockingTime extends ServerResource {

    @Post
    public Object store(String fmJson) {
        System.out.printf("Received json string is:"+fmJson);
        JSONObject blockTimeRequest=new JSONObject(fmJson);
        String bTime=blockTimeRequest.getString("blocktime");
        System.out.printf("blocking time requested is"+ bTime);
        long blockTime=Long.parseLong(bTime);

        ElephantControlSwitch.BLOCK_TIME_VALUE=blockTime;
        setStatus(Status.SUCCESS_OK);
        System.out.println("The new elephantflow blockingTime is:"+ ElephantControlSwitch.BLOCK_TIME_VALUE);
        return "{\"status\" : \"SUCCESS\", \"Details\": \"Elephant Blocking Time set to "+bTime+"\"}";

    }


}
