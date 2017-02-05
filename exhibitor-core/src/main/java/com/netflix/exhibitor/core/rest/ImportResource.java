package com.netflix.exhibitor.core.rest;

import com.netflix.exhibitor.core.importandexport.Importer;
import com.sun.jersey.multipart.FormDataParam;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import java.io.InputStream;

@Path("exhibitor/v1/import")
public class ImportResource {

    private static final String FIELD_BASE_PATH = "basePath";
    private static final String FIELD_OVERWRITE = "overwrite";
    private static final String FIELD_NODES = "nodes";

    private final UIContext context;

    public ImportResource(@Context ContextResolver<UIContext> resolver)
    {
        context = resolver.getContext(UIContext.class);
    }

    @POST
    @Path("json")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response doImport(String json) throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        final JsonNode tree = mapper.readTree(mapper.getJsonFactory().createJsonParser(json));
        final JsonNode basePath = tree.get(FIELD_BASE_PATH);
        final JsonNode overwrite = tree.get(FIELD_OVERWRITE);
        final JsonNode nodes = tree.get(FIELD_NODES);

        if (basePath == null || overwrite == null || nodes == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }

        Importer importer = new Importer(context);
        importer.doImport(basePath.getTextValue(), overwrite.getBooleanValue(), nodes.getElements());

        return Response.ok().build();
    }


    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response doImportFormParams(@FormDataParam("import-file") InputStream file
            , @FormDataParam("base-path") String basePath
            , @FormDataParam("overwrite") Boolean overwrite) throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        final JsonNode tree = mapper.readTree(mapper.getJsonFactory().createJsonParser(file));

        ObjectNode importJson = JsonNodeFactory.instance.objectNode();
        importJson.put(FIELD_BASE_PATH, basePath);
        importJson.put(FIELD_OVERWRITE, overwrite);
        importJson.put(FIELD_NODES, tree);

        return doImport(importJson.toString());
    }
}
