/*
 * iprogram.hpp
 *
 *  Created on: May 8, 2012
 *      Author: daniel
 */

#ifndef DBTOASTER_IPROGRAM_H
#define DBTOASTER_IPROGRAM_H

#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>


namespace dbtoaster {

class IProgram {
public:
	IProgram() :
		running(false)
		, snapshot_request(false)
		, snapshot_ready(true)
		, snapshot_arg(0)
	{
	}
	virtual ~IProgram() {
	}

	bool is_running(){ return running; }
	void start_running()
	{
		running_mtx.lock();
		running = true;
		running_mtx.unlock();
	}
	void stop_running()
	{
		running_mtx.lock();
		if( snapshot_request )
		{
			assert( snapshot_ready == false );
			take_snapshot(snapshot_arg);
			snapshot_request = false;

			{
				boost::lock_guard<boost::mutex> lock(snapshot_ready_mtx);
				snapshot_ready=true;
			}
			snapshot_ready_cond.notify_all();
		}
		running = false;
		running_mtx.unlock();
	}

	virtual int run() = 0;

	boost::unique_future<int> run_async() {
		start_running();
		boost::packaged_task<int> pt(boost::bind(&IProgram::run, this));
		boost::unique_future<int> f = pt.get_future();
		boost::thread task(boost::move(pt));

		return boost::unique_future<int>(
				(boost::detail::thread_move_t<boost::unique_future<int> >) f);
	}

	void request_snapshot(void* _snapshot_arg)
	{
		assert( snapshot_request == false );
		assert( snapshot_ready == true );

		running_mtx.lock();
		if( is_running() )
		{
			snapshot_ready = false;
			snapshot_request = true;
			snapshot_arg = _snapshot_arg;
		}
		else
		{
			take_snapshot(_snapshot_arg);
		}
		running_mtx.unlock();
	}

	void wait_for_snapshot()
	{
		boost::unique_lock<boost::mutex> lock(snapshot_ready_mtx);
		while(!snapshot_ready)
		{
			snapshot_ready_cond.wait(lock);
		}
	}

protected:
	virtual void take_snapshot(void*) = 0;

	bool running;
	boost::mutex running_mtx;

	boost::condition_variable snapshot_ready_cond;
	boost::mutex snapshot_ready_mtx;
	bool snapshot_ready;

	bool snapshot_request;
	void* snapshot_arg;
};
}

#endif /* DBTOASTER_DBT_IPROGRAM_H */
