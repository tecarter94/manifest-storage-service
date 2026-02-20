package org.jboss.sbomer.manifest.storage.service.adapter.in.rest;

import static io.restassured.RestAssured.given;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static jakarta.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.jboss.sbomer.manifest.storage.service.adapter.out.exception.StorageAccessException;
import org.jboss.sbomer.manifest.storage.service.adapter.out.exception.StorageFileNotFoundException;
import org.jboss.sbomer.manifest.storage.service.core.port.api.StorageAdministration;
import org.junit.jupiter.api.Test;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class ExceptionMapperTest {

    @InjectMock
    StorageAdministration storageService;

    @Test
    void testStorageFileNotFoundReturnsNotFound() {
        String message = "File not found";
        when(storageService.getFileContent(anyString()))
                .thenThrow(new StorageFileNotFoundException(message, null));
        given()
                .when().get("/api/v1/storage/content/gen-1/bom.json")
                .then()
                .statusCode(NOT_FOUND.getStatusCode())
                .body(equalTo(message));
    }

    @Test
    void testStorageAccessDeniedReturnsForbidden() {
        String message = "Access denied";
        when(storageService.getFileContent(anyString()))
                .thenThrow(new StorageAccessException(message, null));
        given()
                .when().get("/api/v1/storage/content/gen-1/bom.json")
                .then()
                .statusCode(FORBIDDEN.getStatusCode())
                .body(equalTo(message));
    }

    @Test
    void testUnhandledExceptionReturnsInternalServerError() {
        String message = "Error";
        when(storageService.getFileContent(anyString()))
                .thenThrow(new RuntimeException(message));
        given()
                .when().get("/api/v1/storage/content/gen-1/bom.json")
                .then()
                .statusCode(INTERNAL_SERVER_ERROR.getStatusCode())
                .body(equalTo(message));
    }
}