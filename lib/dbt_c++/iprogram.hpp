/*
 * iprogram.hpp
 *
 *  Created on: May 8, 2012
 *      Author: daniel
 */

#ifndef DBTOASTER_IPROGRAM_H
#define DBTOASTER_IPROGRAM_H

#include <string>
#include <vector>

#include <boost/any.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>


namespace dbtoaster {


/**
 * Type definitions of data-structures used for representing events.
 */

typedef int relation_id_t;

enum event_type { insert_tuple = 0, delete_tuple, system_ready_event };
std::string event_name[] = {
    std::string("insert"), 
    std::string("delete"), 
    std::string("system_ready")
};

typedef std::vector<boost::any> event_args_t;

/**
 * Data-structure used for representing a event consisting of: event type,
 * relation identifier corresponding to the stream/table it relates to and
 * finally, the tuple associated with event.
 */
struct event_t
{
    event_type type;
    relation_id_t id;
    event_args_t data;

    event_t(event_type t, relation_id_t i, event_args_t& d)
    : type(t), id(i), data(d)
    {}
};



/**
 * IProgram is the base class for executing sql programs. It provides
 * functionality for running the program in synchronous or asynchronous mode
 * and recording intermediate or final snapshots of the results.
 *
 * The 'TLQ_T' class parameter represents the data-structure used for 
 * storing the results of the program.
 */
template<class TLQ_T>
class IProgram {
public:
    typedef boost::shared_ptr<TLQ_T> snapshot_t;

    IProgram() :
        running(false)
        , finished(false)
        , snapshot_request(false)
        , snapshot_ready(true)
    {}
    virtual ~IProgram() {
    }

    /**
     * This should get overridden by a function that does initialization work,
     * such as processing the static table tuples.
     */
    virtual void init() = 0;

    /**
     * Executes the program by launching the virtual method 'process_streams()'.
     * @param async If 'true' the execution is performed in a separate thread.
     */
    void run( bool async = false ) {
        if( async )
        {
            boost::packaged_task<void> pt(boost::bind(&IProgram::run, 
                                                      this, false));
            boost::thread task( boost::move(pt) );
        }
        else
        {
            start_running();
            process_streams();
            finished = true;
            stop_running();
        }
    }

    /**
     * This function provides a way for testing whether the program has 
     * finished or not when run in asynchronous mode.
     * @return 'true' if the program has finished execution.
     */
    bool is_finished()
    {
        return finished;
    }

    /**
     * Obtains a snapshot of the results of the program. If the program is
     * currently running in asynchronous mode, it will make sure that the
     * snapshot is consistent.
     * @return A snapshot of the 'TLQ_T' data-structure representing 
     *         the results of the program.
     */
    snapshot_t get_snapshot()
    {
        if( !is_finished() )
        {
            request_snapshot();
            return wait_for_snapshot();
        }
        else
            return take_snapshot();
    }

protected:
    /**
     * This should get overridden by a function that reads stream events and
     * processes them by calling process_stream_event().
     */
    virtual void process_streams() = 0;

    /**
     * This should get overridden by a function that processes an event by
     * calling the appropriate trigger. This function can also be used for
     * performing additional tasks before or after processing an event, such
     * as handling requests for consistent snapshots of the results.
     * In order to preserve functionality, functions that override it should
     * call their base class variants.
     * @param ev The event being processed.
     */
    virtual void process_stream_event(event_t& ev)
    {
        process_snapshot();
    }

    /**
     * Virtual function that should implement the functionality of taking
     * snapshots of the results of the program.
     * @return The collected snapshot.
     */
    virtual snapshot_t take_snapshot() = 0;

    /**
     * Tests the running state of the program.
     * @return 'true' if the program is currently being executed.
     */
    bool is_running(){ return running; }

    /**
     * Signal the beginning of the execution of the program.
     */
    void start_running()
    {
        running_mtx.lock();
        running = true;
        running_mtx.unlock();
    }
    /**
     * Signal the end of the execution of the program.
     */
    void stop_running()
    {
        running_mtx.lock();
        process_snapshot();
        running = false;
        running_mtx.unlock();
    }

    /**
     * Function for processing requests for program results snapshot.
     * Gets executed only between the processing of events and not during,
     * in order to get consistent results.
     */
    void process_snapshot()
    {
        if( snapshot_request )
        {
            assert( snapshot_ready == false );
            snapshot = take_snapshot();
            snapshot_request = false;

            {
                boost::lock_guard<boost::mutex> lock(snapshot_ready_mtx);
                snapshot_ready=true;
            }
            snapshot_ready_cond.notify_all();
        }
    }

    /*
     * Function for recording a request for a snapshot.
     */
    void request_snapshot()
    {
        assert( snapshot_request == false );
        assert( snapshot_ready == true );
        if( snapshot_request || !snapshot_ready )   return;

        running_mtx.lock();
        if( is_running() )
        {
            snapshot_ready = false;
            snapshot_request = true;
        }
        else
        {
            snapshot = take_snapshot();
        }
        running_mtx.unlock();
    }

    /**
     * Function for waiting for a previously recorded snapshot request to
     * complete.
     * @return The snapshot taken as a response of a previously recorded 
     *         request.
     */
    snapshot_t wait_for_snapshot()
    {
        if( !snapshot_ready )
        {
            boost::unique_lock<boost::mutex> lock(snapshot_ready_mtx);
            while(!snapshot_ready)
            {
                snapshot_ready_cond.wait(lock);
            }
        }

        snapshot_t result = snapshot;
        snapshot = snapshot_t();
        return result;
    }

private:
    bool running;
    bool finished;
    boost::mutex running_mtx;

    boost::condition_variable snapshot_ready_cond;
    boost::mutex snapshot_ready_mtx;
    bool snapshot_ready;

    bool snapshot_request;
    snapshot_t snapshot;
};
}

#endif /* DBTOASTER_DBT_IPROGRAM_H */
