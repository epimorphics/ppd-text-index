/*****************************************************************************
 * File:    ProgressMonitor.java
 * Project: lr-text-indexer
 * Created: 14 Nov 2014
 * By:      ian
 *
 * Copyright (c) 2014 Epimorphics Ltd. All rights reserved.
 *****************************************************************************/

// Package
///////////////

package com.epimorphics.lr.jena.query.text;


// Imports
///////////////


/**
 * <p>TODO class comment</p>
 * 
 * @author Ian Dickinson, Epimorphics (mailto:ian@epimorphics.com)
 */
// TDBLoader has a similar progress monitor
// Not used here to avoid making ARQ dependent on TDB
// So potential to rationalise and put progress monitor in a common
// utility class
public class ProgressMonitor
{
    String progressMessage;
    long startTime;
    long progressCount;
    long intervalStartTime;
    long progressAtStartOfInterval;
    long reportingInterval = 10000; // milliseconds

    public ProgressMonitor( String progressMessage ) {
        this.progressMessage = progressMessage;
        start(); // in case start not called
    }

    void start() {
        startTime = System.currentTimeMillis();
        progressCount = 0L;
        startInterval();
    }

    private void startInterval() {
        intervalStartTime = System.currentTimeMillis();
        progressAtStartOfInterval = progressCount;
    }

    void progressByOne() {
        progressByN( 1 );
    }

    void progressByN( int n ) {
        progressCount += n;
        long now = System.currentTimeMillis();
        if (reportDue( now )) {
            report( now );
            startInterval();
        }
    }

    boolean reportDue( long now ) {
        return now - intervalStartTime >= reportingInterval;
    }

    private void report( long now ) {
        long progressThisInterval = progressCount - progressAtStartOfInterval;
        long intervalDuration = now - intervalStartTime;
        long intervalSeconds = intervalDuration / 1000;
        if (intervalSeconds == 0)
            intervalSeconds = 1;
        long overallDuration = now - startTime;
        String message = progressCount + " (" + progressThisInterval / intervalSeconds + " per second) "
                + progressMessage + " (" + progressCount / Math.max( overallDuration / 1000, 1 )
                + " per second overall)";
        TextIndexer.log.info( message );
    }

    public void close() {
        long overallDuration = System.currentTimeMillis() - startTime;
        String message = progressCount + " (" + progressCount / Math.max( overallDuration / 1000, 1 )
                + " per second) " + progressMessage;
        TextIndexer.log.info( message );
    }
}
