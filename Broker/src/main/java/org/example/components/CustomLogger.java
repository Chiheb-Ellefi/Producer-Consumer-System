package org.example.components;

import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class CustomLogger {
    private final Logger log;
    private final Semaphore mutex;

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public CustomLogger() {
        log = Logger.getLogger(CustomLogger.class.getName());
        mutex = new Semaphore(1);
    }

    public void info(String msg) {
        try {
            mutex.acquire();
            log.info(ANSI_BLUE + msg + ANSI_RESET);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.severe("Logging interrupted: " + e.getMessage());
        } finally {
            mutex.release();
        }
    }

    public void error(String msg) {
        try {
            mutex.acquire();
            log.severe(ANSI_RED + msg + ANSI_RESET);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.severe("Logging interrupted: " + e.getMessage());
        } finally {
            mutex.release();
        }
    }

    public void warning(String msg) {
        try {
            mutex.acquire();
            log.warning(ANSI_YELLOW + msg + ANSI_RESET);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.severe("Logging interrupted: " + e.getMessage());
        } finally {
            mutex.release();
        }
    }
}