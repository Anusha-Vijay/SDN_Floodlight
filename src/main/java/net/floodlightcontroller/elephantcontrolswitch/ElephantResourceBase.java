package net.floodlightcontroller.elephantcontrolswitch;

import org.restlet.resource.ServerResource;

/**
 * Created by Anushavijay on 12/8/16.
 */
public class ElephantResourceBase extends ServerResource {
    IElephantService getElephantService() {
        return (IElephantService) getContext().getAttributes().
                get(IElephantService.class.getCanonicalName());
    }
}
