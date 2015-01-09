package com.epimorphics.lr.jena.query.text;

import org.slf4j.Logger;


/**
 * Progress monitor, to report incremental progress through text indexing operation.
 *
 * TDBLoader has a similar progress monitor, but it is
 * not used here to avoid making ARQ dependent on TDB
 */
public class ProgressMonitor
{
    /***********************************/
    /* Constants                       */
    /***********************************/

    public static final int MILLISECONDS = 1000;

    public static final int DEFAULT_REPORTING_INTERVAL = 10 * MILLISECONDS;

    /***********************************/
    /* Static variables                */
    /***********************************/

    /***********************************/
    /* Instance variables              */
    /***********************************/

    private String progressMessage;

    private long startTime;
    private long intervalStartTime;

    private long progressCount;
    private long progressAtStartOfInterval;

    private Logger log;

    /***********************************/
    /* Constructors                    */
    /***********************************/

    public ProgressMonitor( String progressMessage, Logger log ) {
        this.progressMessage = progressMessage;
        this.log = log;
        init();
    }

    /***********************************/
    /* External signature methods      */
    /***********************************/

    /**
     * @return The number of seconds to wait before reporting
     */
    public int reportingInterval() {
        return DEFAULT_REPORTING_INTERVAL;
    }

    /**
     * Reset counts and times. May be safely called more than once.
     */
    public void init() {
        startTime = System.currentTimeMillis();
        progressCount = 0L;
        startInterval();
    }

    /**
     * Advance progress by one unit
     */
    public void progressByOne() {
        progressByN( 1 );
    }

    /**
     * Advance progress by n units
     * @param n
     */
    public void progressByN( int n ) {
        progressCount += n;
        long now = System.currentTimeMillis();
        if (reportDue( now )) {
            report( now );
            startInterval();
        }
    }

    /**
     * Make the final entry
     */
    public void close() {
        long now = System.currentTimeMillis();
        String message = String.format( "%d (%.2f per second)", // does two decimal places make sense here?
                                        progressCount,
                                        (float) progressCount / (float) overallDurationRounded( now ));
        log.info( message );
    }

    /***********************************/
    /* Internal implementation methods */
    /***********************************/

    protected void startInterval() {
        intervalStartTime = System.currentTimeMillis();
        progressAtStartOfInterval = progressCount;
    }

    protected boolean reportDue( long now ) {
        return now - intervalStartTime >= reportingInterval();
    }

    protected void report( long now ) {
        long progressThisInterval = progressCount - progressAtStartOfInterval;

        String message = String.format( "%d (%.2f per second) %s (%.2f per second overall)", // does two decimal places make sense?
                                        progressCount,
                                        (float) progressThisInterval / (float) intervalSecondsRounded( now ),
                                        progressMessage,
                                        (float) progressCount / (float) overallDurationRounded( now )
                                        );
        log.info( message );
    }

    protected long intervalSecondsRounded( long now ) {
        return roundedSeconds( now - intervalStartTime );
    }

    protected long overallDurationRounded( long now ) {
        return roundedSeconds( now - startTime );
    }

    /**
     * Round to nearest number of seconds, with minimum 1
     * @param ms Duration in milliseconds
     * @return Duration in seconds, min 1L
     */
    protected long roundedSeconds( long ms ) {
        return Math.round( Math.max( 1L, seconds( ms ) ) );
    }

    protected long seconds( long ms ) {
        return ms / MILLISECONDS;
    }


    /***********************************/
    /* Inner class definitions         */
    /***********************************/


}
