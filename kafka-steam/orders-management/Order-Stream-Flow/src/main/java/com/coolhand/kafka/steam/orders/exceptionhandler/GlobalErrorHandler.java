package com.coolhand.kafka.steam.orders.exceptionhandler;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalErrorHandler {
    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail handleIllegalStateException(IllegalStateException exception){

        var problemDetails=ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400),
                exception.getMessage());
        problemDetails.setProperty("AdditionalInfo" , "Please pass a valid orderType : general / restaurant");
        return  problemDetails;

    }
}
