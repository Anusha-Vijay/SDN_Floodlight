package net.floodlightcontroller.elephantcontrolswitch;

import org.json.JSONObject;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;

/**
 * Created by Anushavijay on 12/8/16.
 */
public class ElephantResourceBase extends ServerResource{
    protected static Logger logger;

    @Post
    public void store(String fmJson) {
        System.out.printf("Received json string is:"+fmJson);
        JSONObject bandwidthRequest=new JSONObject(fmJson);
        String bandwidth=bandwidthRequest.getString("bandwidth");
        System.out.printf("Bandwidth requested is"+ bandwidth);


        //parse the json element

    }




}
