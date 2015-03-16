package org.mayo.camel.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path("/")
public class FSSService {

    public FSSService() {
        // nothing
    }

    @GET
    @Path("/{resource}/{id}")
    public void getResource(@PathParam("resource") String resource, @PathParam("id") String id) {
        // nothing
    }


}
