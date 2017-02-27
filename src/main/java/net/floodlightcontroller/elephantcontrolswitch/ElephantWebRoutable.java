package net.floodlightcontroller.elephantcontrolswitch;

import net.floodlightcontroller.restserver.RestletRoutable;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.routing.Router;

/**
 * Created by Anushavijay on 12/8/16.
 */
public class ElephantWebRoutable implements RestletRoutable {
    @Override
    public Restlet getRestlet(Context context) {
        Router router= new Router(context);
        router.attach("/bandwidth/json",ElephantBandwidth.class);
        router.attach("/blockingtime/json",ElephantBlockingTime.class);
        return router;
    }

    @Override
    public String basePath() {
        return "/elephantflow";
    }
}
