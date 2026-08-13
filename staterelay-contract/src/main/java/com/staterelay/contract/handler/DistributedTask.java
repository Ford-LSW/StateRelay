package com.staterelay.contract.handler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the stable handler name that a Worker exposes to StateRelay.
 *
 * <p>The value is part of the dispatch protocol and must remain unique within an
 * application so commands can be routed to the intended {@link TaskHandler}.</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DistributedTask {

    String value();
}
