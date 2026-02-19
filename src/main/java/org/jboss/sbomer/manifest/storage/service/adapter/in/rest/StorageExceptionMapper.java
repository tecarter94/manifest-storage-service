package org.jboss.sbomer.manifest.storage.service.adapter.in.rest;

import org.jboss.sbomer.manifest.storage.service.adapter.out.exception.StorageException;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import lombok.extern.slf4j.Slf4j;

@Provider
@Slf4j
public class StorageExceptionMapper implements ExceptionMapper<StorageException> {

    @Override
    public Response toResponse(StorageException e) {
        log.error("Storage operation failed: {}", e.getMessage(), e);
        return Response.status(e.getStatus()).entity(e.getMessage()).build();
    }
}