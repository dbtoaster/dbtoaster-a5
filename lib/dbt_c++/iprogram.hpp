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
		, tuple_count(0)
		, refresh_request(false)
		, view_ready(true)
		, view_ts(-1)
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
		if( refresh_request )
		{
			assert( view_ready == false );
			if( view_ts != tuple_count )
			{
				refresh_maps();
				view_ts = tuple_count;
			}
			refresh_request = false;
			{
				boost::lock_guard<boost::mutex> lock(view_ready_mtx);
				view_ready=true;
			}
			view_ready_cond.notify_all();
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

	virtual void refresh_maps() = 0;

	void request_refresh()
	{
		assert( refresh_request == false );
		assert( view_ready == true );

		running_mtx.lock();
		if( is_running() )
		{
			view_ready = false;
			refresh_request = true;
		}
		else if( view_ts != tuple_count )
		{
			refresh_maps();
			view_ts = tuple_count;
		}
		running_mtx.unlock();
	}

	void wait_for_refresh()
	{
		boost::unique_lock<boost::mutex> lock(view_ready_mtx);
		while(!view_ready)
		{
			view_ready_cond.wait(lock);
		}
	}

protected:
	bool running;
	boost::mutex running_mtx;

	boost::condition_variable view_ready_cond;
	boost::mutex view_ready_mtx;
	bool view_ready;

	unsigned int tuple_count;

	bool refresh_request;
	unsigned int view_ts;
};
}

#endif /* DBTOASTER_DBT_IPROGRAM_H */
