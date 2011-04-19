#ifndef DBTOASTER_PROFILER_H
#define DBTOASTER_PROFILER_H

#include <iostream>
#include <fstream>

#ifdef ENABLE_PROFILER
#include <map>
#include <list>
#include <utility>

#include <boost/any.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

// Profiling types and service interface (statically generated from Thrift)
#include "protocol/gen-cpp/Profiler.h"

namespace DBToaster
{
    namespace Profiler
    {
        using namespace std;
        using namespace boost;

        using boost::shared_ptr;

        using namespace apache::thrift;
        using namespace apache::thrift::protocol;
        using namespace apache::thrift::transport;
        using namespace apache::thrift::server;

        typedef string StatisticName;
        typedef int32_t CodeLocation;

        // Polymorphic sample type, and type creation.
        typedef boost::any SampleUnits;

        // Circular buffer size
        int32_t bufferSize = 20;

        // Includes here since sample units need to be defined first.
        #if defined (__APPLE__) && defined(__MACH__) && !defined(__powerpc__) && !defined( __ppc__ ) && !defined( __ppc64__ )
        #include "profiler_osx.h"
        #elif defined (unix) || defined(__unix) || defined(__unix__)
        #include "profiler_posix.h"
        #endif

        // Comparison implementations for protocol.
        /*
        inline bool operator<(const Protocol::ProfileLocation& l, const Protocol::ProfileLocation& r) const
        {
            return ( (l.statName < r.statName) ||
                     (l.statName == r.statName && l.codeLocation < r.codeLocation) );            
        }
        */

        // Protocol constructors for sample types
        Protocol::SampleUnits createExecTimeProtocolSample(const boost::any& sample)
        {
            const timespec& tSample = boost::any_cast<timespec>(sample);

            Protocol::SampleUnits r;
            r.sampleType = Protocol::EXEC_TIME;
            r.execTime.tv_sec = tSample.tv_sec;
            r.execTime.tv_nsec = tSample.tv_nsec;
            r.execTime.__isset.tv_sec = true;
            r.execTime.__isset.tv_nsec = true;
            r.__isset.sampleType = true;
            r.__isset.execTime = true;
            return r;
        }

        // Profiler key types.
        struct ProfileLocationKey
        {
            StatisticName statName;
            CodeLocation locationId;
            ProfileLocationKey(StatisticName s, CodeLocation l)
                : statName(s), locationId(l)
            {}

            Protocol::ProfileLocation createProtocolProfileLocation() const
            {
                Protocol::ProfileLocation r;
                r.statName = statName;
                r.codeLocation = locationId;
                return r;
            }

            string as_string() const {
                ostringstream oss;
                oss << statName << ", " << locationId;
                return oss.str();
            }
        };

        inline bool operator<(const ProfileLocationKey& l, const ProfileLocationKey& r)
        {
            return ( (l.statName < r.statName) ||
                     (l.statName == r.statName && l.locationId < r.locationId) );
        }

        // Sampling strategies
        namespace HandlerSampler
        {
            struct HandlerSampleKey {
                StatisticName statName;
                int32_t handlerId;
                HandlerSampleKey(StatisticName s, int32_t h)
                    : statName(s), handlerId(h)
                {}
            };

            inline bool operator<(const HandlerSampleKey& l, const HandlerSampleKey& r)
            {
                return ( (l.statName < r.statName) ||
                         (l.statName == r.statName && l.handlerId < r.handlerId) );
            }

            struct HandlerSampleKeyConverter
            {
                typedef map<CodeLocation, int32_t> LocationHandlers;
                LocationHandlers codeHandlers;
                HandlerSampleKey operator()(const ProfileLocationKey& k)
                {
                    assert ( codeHandlers.find(k.locationId) != codeHandlers.end() );
                    HandlerSampleKey r(k.statName, codeHandlers[k.locationId]);
                    return r;
                }
            };
        };

        namespace LocationSampler
        {
            struct LocationSampleKey {
                StatisticName statName;
                CodeLocation locationId;
                
                LocationSampleKey(StatisticName s, CodeLocation l) : statName(s), locationId(l) {}

                string as_string() const {
                    ostringstream oss;
                    oss << statName << ", " << locationId;
                    return oss.str();
                }
            };

            inline bool operator<(const LocationSampleKey& l, const LocationSampleKey& r)
            {
                return ( (l.statName < r.statName) ||
                         (l.statName == r.statName && l.locationId < r.locationId) );
            }

            struct LocationSampleKeyConverter
            {
                LocationSampleKey operator()(const ProfileLocationKey& k)
                {
                    LocationSampleKey r(k.statName, k.locationId);
                    return r;
                }
            };
        };

        struct SampleFunctions
        {
            typedef boost::function<boost::any (const boost::any&)> SampleConstructor;
            typedef boost::function<Protocol::SampleUnits (const boost::any&)> ProtocolConstructor;

            SampleConstructor sampleConstructor;
            ProtocolConstructor protocolConstructor;
        };


        // Unlocked statistics data structure
        // Define a profile data structure with virtual methods to enable
        // subclasses to define their own locking strategies.
        template<typename SampleStrategyKey, typename KeyConverter>
        class Profile
        {
        public:
            typedef map<StatisticName, SampleFunctions> SampleHelpers;

            // Separate timestamps and sample buffers
            typedef map<SampleStrategyKey, Timestamp> ProfileTimestamps;

            typedef boost::circular_buffer<SampleUnits> ProfileBuffer;
            typedef map<ProfileLocationKey, shared_ptr<ProfileBuffer> > StatisticsProfile;

            Profile(KeyConverter k) : keyConverter(k) { init(lastSamplePoint); }
            virtual ~Profile() {}

            void addSampleFunctions(StatisticName statsName, SampleFunctions sf)
            {
                sampleFns[statsName] = sf;
            }

            SampleFunctions::SampleConstructor
            getSampleConstructor(StatisticName statsName)
            {
                assert ( sampleFns.find(statsName) != sampleFns.end() );
                return sampleFns[statsName].sampleConstructor;
            }

            SampleFunctions::ProtocolConstructor
            getProtocolConstructor(StatisticName statsName)
            {
                assert ( sampleFns.find(statsName) != sampleFns.end() );
                return sampleFns[statsName].protocolConstructor;
            }

            virtual Timestamp getLastUpdate(const SampleStrategyKey& sampleKey)
            {
                Timestamp r;
                init(r);

                typename ProfileTimestamps::iterator statIt =
                    statsTimestamps.find(sampleKey);
                if ( statIt != statsTimestamps.end()  )
                    r = statIt->second;
                
                return r;
            }

            virtual void setLastUpdate(const SampleStrategyKey& sampleKey, const Timestamp& sampleTime)
            {
                statsTimestamps[sampleKey] = sampleTime;

                if ( lastSamplePoint < sampleTime )
                    lastSamplePoint = sampleTime;
            }

            virtual void addProfileSample(const ProfileLocationKey& locationKey,
                                          const SampleUnits& sample)
            {
                shared_ptr<ProfileBuffer> buffer = statsProfile[locationKey];
                if ( !buffer ) {
                    buffer = shared_ptr<ProfileBuffer>(new ProfileBuffer(bufferSize));
                    buffer->push_back(sample);
                    statsProfile[locationKey] = buffer;
                }
                else {
                    buffer->push_back(sample);
                }

                //cout << "Buffer " << locationKey.as_string()
                //    << " adding sample, "
                //    << " size " << buffer->size() << endl;
            }

            // Synchronizes sample buffers to the same slot in the circular buffer.
            virtual Timestamp synchronizeSamples()
            {
                //cout << "Getting ceiling timestamp" << endl;

                list<ProfileLocationKey> deletedLocations;
                Timestamp syncPoint = ceil(lastSamplePoint, sampleFreq);

                //cout << "Syncing over statsProfile, size: " << statsProfile.size() << endl;

                typename StatisticsProfile::iterator statIt = statsProfile.begin();
                typename StatisticsProfile::iterator statEnd = statsProfile.end();
                for (; statIt != statEnd; ++statIt)
                {
                    const ProfileLocationKey& locationKey = statIt->first;
                    shared_ptr<ProfileBuffer> buffer = statIt->second;

                    //cout << "Finding timestamp for key." << endl;

                    SampleStrategyKey sampleKey = keyConverter(locationKey);
                    typename ProfileTimestamps::iterator timestampIt = statsTimestamps.find(sampleKey);

                    if ( timestampIt == statsTimestamps.end() ) {
                        cerr << "No timestamp found for profile location!" << endl;
                        exit(1);
                    }

                    //cout << "Computing buffer distance, on buffer for loc: "
                    //    << locationKey.as_string()
                    //    << " , size " << buffer->size() << endl;

                    Timestamp bufferPoint = timestampIt->second;
                    Timestamp dist = diff(syncPoint, bufferPoint);
                    ProfileBuffer::size_type steps = divide(dist, sampleFreq);

                    //cout << "Running circular buffer for dist: " << dist
                    //    << " syncPoint: " << syncPoint
                    //    << " bufferPoint: " << bufferPoint
                    //    << ", steps: " << steps << endl;

                    if ( steps >= buffer->size() ) {
                        buffer->clear();
                        deletedLocations.push_back(locationKey);
                    }

                    else {
                        buffer->erase(buffer->begin(), buffer->begin()+steps);
                    }
                }

                //cout << "Deleting for " << deletedLocations.size() << " locations" << endl;

                list<ProfileLocationKey>::iterator delIt = deletedLocations.begin();
                list<ProfileLocationKey>::iterator delEnd = deletedLocations.end();
                for (; delIt != delEnd; ++delIt)
                    statsProfile.erase(*delIt);

                //cout << "Returning syncpoint" << endl;

                return syncPoint;
            }


            // Protocol copiers for profiler types
            void copyProfileBuffer(StatisticsProfile::const_iterator statProfileIt,
                                   Protocol::ProfileBuffer& destBuffer)
            {
                const StatisticName& statName = statProfileIt->first.statName;
                shared_ptr<ProfileBuffer> buffer = statProfileIt->second;
    
                SampleFunctions::ProtocolConstructor createProtocolSample =
                    getProtocolConstructor(statName);

                if ( buffer )
                {
                    // Ensure capacity up front to avoid dynamic resizing.
                    destBuffer.reserve(buffer->size());

                    ProfileBuffer::const_iterator bufIt = buffer->begin();
                    ProfileBuffer::const_iterator bufEnd = buffer->end();

                    //cout << "Copying buffer of size " << buffer->size() << endl;

                    for (; bufIt != bufEnd; ++bufIt)
                        destBuffer.push_back(createProtocolSample(*bufIt));

                    //cout << "Dest buffer size " << destBuffer.size() << endl;
                }
            }

            virtual void copyStatisticsProfile(Protocol::StatisticsProfile& dest)
            {
                //cout << "Synchronizing samples" << endl;
                Timestamp syncPoint = synchronizeSamples();
                copyTimestampProtocol(syncPoint, dest.t);

                //cout << "Copied sync time" << endl;
                //cout << "Copying statsProfile with #locations: "
                //    << statsProfile.size() << endl;

                StatisticsProfile::const_iterator statIt = statsProfile.begin();
                StatisticsProfile::const_iterator statEnd = statsProfile.end();
                for (; statIt != statEnd; ++statIt)
                {
                    Protocol::ProfileLocation destLocKey =
                        statIt->first.createProtocolProfileLocation();

                    //cout << "Copying buffer for location "
                    //    << destLocKey.statName << ", "
                    //    << destLocKey.codeLocation << endl;

                    Protocol::ProfileBuffer& destBuffer = dest.profile[destLocKey];
                    copyProfileBuffer(statIt, destBuffer);

                    //cout << "Result buffer size for location "
                    //    << destLocKey.statName << ", "
                    //    << destLocKey.codeLocation << ": "
                    //    << destBuffer.size() << endl;
                }

                //cout << "Done copying stats profile" << endl;
            }

        public:
            SampleHelpers sampleFns;
            Timestamp lastSamplePoint;

            ProfileTimestamps statsTimestamps;
            StatisticsProfile statsProfile;

            KeyConverter keyConverter;
        };

        template<typename SampleStrategyKey, typename KeyConverter>
        class LockedProfile : public Profile<SampleStrategyKey, KeyConverter>
        {
            boost::mutex profileMutex;
        public:
            LockedProfile(KeyConverter k) : Profile<SampleStrategyKey, KeyConverter>(k) {}
            ~LockedProfile() {}

            // No need to lock since we assume handlers for the same
            // query cannot run in parallel
            Timestamp getLastUpdate(const SampleStrategyKey& sampleKey) 
            {
                return Profile<SampleStrategyKey, KeyConverter>::getLastUpdate(sampleKey);
            }

            // Ensure no copying stats while adding samples
            void setLastUpdate(const SampleStrategyKey& sampleKey, const Timestamp& sampleTime)
            {
                boost::mutex::scoped_lock updateLock(profileMutex);
                Profile<SampleStrategyKey, KeyConverter>::setLastUpdate(sampleKey, sampleTime);
            }

            // Ensure no copying stats while adding samples
            void addProfileSample(const ProfileLocationKey& locationKey,
                                  const SampleUnits& sample)
            {
                boost::mutex::scoped_lock addSampleLock(profileMutex);
                Profile<SampleStrategyKey, KeyConverter>::addProfileSample(locationKey, sample);
           }

            // No adding samples/timestamp updates while copying
            void copyStatisticsProfile(Protocol::StatisticsProfile& dest)
            {
                boost::mutex::scoped_lock syncAndCopyLock(profileMutex);
                Profile<SampleStrategyKey, KeyConverter>::copyStatisticsProfile(dest);
            }
        };

        // Locking modes
        #ifdef USE_FULL_HANDLER_LOCKING
            boost::mutex handlerMutex;
            #define LOCK_HANDLER_PROFILE boost::mutex::scoped_lock lock(handlerMutex);
            #define UNLOCK_HANDLER_PROFILE
        #else
            #define LOCK_HANDLER_PROFILE
            #define UNLOCK_HANDLER_PROFILE
        #endif

        // Profiling macros
        // Note these only work for CPU now due to invoking constructor from timestamps.

        // Profile point level sampling.
        #define START_HANDLER_SAMPLE(statsType, handlerId)

        #define END_HANDLER_SAMPLE(statType, handlerId)

        #define START_PROFILE(statsType,codeLoc) \
            Timestamp codeLoc##Var;        \
            init(codeLoc##Var); \
            LocationSampler::LocationSampleKey sampleKey(statsType, codeLoc); \
            Timestamp codeLoc##Last = \
                queryProfile.getLastUpdate(sampleKey); \
            if ( sampleFreq < diff(now(), codeLoc##Last) ) { \
                codeLoc##Var = now(); \
            }

            //cout << "Profiling loc: " << sampleKey.as_string() \
                << " with now: " << codeLoc##Var \
                << " last "<< codeLoc##Last << endl; \

        #define END_PROFILE(statsType,codeLoc)     \
            if ( isValid(codeLoc##Var) ) { \
                Timestamp end = now(); \
                SampleUnits sample = \
                    queryProfile.getSampleConstructor(statsType) \
                        (boost::any(diff(end, codeLoc##Var))); \
                ProfileLocationKey locKey(statsType, codeLoc); \
                queryProfile.addProfileSample(locKey, sample); \
                LocationSampler::LocationSampleKey sampleKey(statsType, codeLoc); \
                queryProfile.setLastUpdate(sampleKey, end); \
            }

            //cout << "Setting loc: " << sampleKey.as_string() \
                << " last update: " << end \
                << " sample: " << to_string(sample) << endl; \

        /*
        // Handler level sampling.
        #define START_HANDLER_SAMPLE(statsType, handlerId)     \
            bool handlerId##Sample = false; \
            HandlerSampler::HandlerSampleKey sampleKey(statsType, handlerId); \
            Timestamp handlerId##Last = queryProfile.getLastUpdate(sampleKey); \
            if ( !(sampleFreq < diff(now(), handlerId##Last) ) { \
                handlerId##Sample = true; \
            }

        #define END_HANDLER_SAMPLE(statsType, handlerId) \
            Timestamp end = now(); \
            HandlerSampler::HandlerSampleKey sampleKey(statsType, handlerId); \
            queryProfile.setLastUpdate(sampleKey, end);

        #define START_PROFILE(statsType, handlerId, codeLoc) \
            Timestamp codeLoc##Var; \
            init(codeLoc##Var); \
            if ( handlerId##Sample ) { \
                codeLoc##Var = now(); \
            }

        #define END_PROFILE(statsType, handlerId, codeLoc) \
            if ( handlerId##Sample ) { \
                Timestamp end = now(); \
                SampleUnits sample = \
                    queryProfile.getSampleConstructor(statsType) \
                        (boost::any(diff(end, codeLoc##Var))); \
                ProfileLocationKey locKey(statsType, codeLoc); \
                queryProfile.addProfileSample(locKey, sample); \
            }
        */


        typedef LockedProfile<LocationSampler::LocationSampleKey,
                              LocationSampler::LocationSampleKeyConverter>
        LockedLocationProfiler;

        typedef LockedProfile<HandlerSampler::HandlerSampleKey,
                              HandlerSampler::HandlerSampleKeyConverter>
        LockedHandlerProfiler;

        LocationSampler::LocationSampleKeyConverter locationSampleKeyConverter;
        LockedLocationProfiler queryProfile(locationSampleKeyConverter);

        // TODO: initialize sample frequency
        //Timestamp sampleFreq;

        void initializeProfiler()
        {
            sampleFreq = 500000000LL;
            SampleFunctions timestampHelpers;
            timestampHelpers.sampleConstructor = &createTimestampSample;
            timestampHelpers.protocolConstructor = &createExecTimeProtocolSample;
            queryProfile.addSampleFunctions("cpu", timestampHelpers);

            // TODO: initialize codeHandlers for HandlerSampleKeyConverter
        }

        // Macros for profiler+profiler service setup from main Thrift service.

        // TODO: auto generate location handlers.
        // TODO: other profiling types, e.g. memory usage.
        #define PROFILER_INITIALIZATION initializeProfiler();

        #define PROFILER_SERVICE_METHOD_IMPLEMENTATION \
        void getStatisticsProfile(Protocol::StatisticsProfile& dest) \
        { \
            cout << "Inside stats profiling." << endl; \
            LOCK_HANDLER_PROFILE \
            queryProfile.copyStatisticsProfile(dest); \
            UNLOCK_HANDLER_PROFILE \
        }

        // Standalone Profiler service
        namespace Protocol
        {
            class ProfilerHandler : virtual public ProfilerIf {
            public: 
                ProfilerHandler() {
                    PROFILER_INITIALIZATION
                }

                PROFILER_SERVICE_METHOD_IMPLEMENTATION
            };

            struct ProfilerService
            {
                int port;
                shared_ptr<boost::thread> serviceThread;

                ProfilerService()
                {
                    port = (70457>>3);;
                    serviceThread = shared_ptr<boost::thread>(new boost::thread(*this));
                }

                void operator()()
                {
                    shared_ptr<ProfilerHandler> handler(new ProfilerHandler());;
                    shared_ptr<TProcessor> processor(new ProfilerProcessor(handler));;
                    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));;
                    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());;
                    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());;
                    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);;
                    server.serve();;
                }
            };
        };

        #define PROFILER_ENGINE_IMPLEMENTATION \
            boost::shared_ptr<Protocol::ProfilerService> profileService(new Protocol::ProfilerService());
    };
};

#elif defined(ENABLE_SIMPLE_PROFILER)

namespace DBToaster
{
    namespace Profiler
    {
        using namespace std;

        inline void accumulate_time_span(
            struct timeval& start, struct timeval& end,
            double& sec_accum, double& usec_accum)
        {
            long sec = end.tv_sec - start.tv_sec;
            long usec = end.tv_usec - start.tv_usec;
            if (usec < 0) { --sec; usec+=1000000; }
            sec_accum += (sec + (usec / 1000000.0));
            usec_accum += usec;
        }

        inline void reset_time_span(const char* counter_name,
                                    unsigned long& counter, double reset_frequency,
                                    double& sec_accum, double& usec_accum,
                                    const char* text, ofstream* log = NULL)
        {
            // Print out:
            // # processed, time per item since last reset, time spent in total
            cout << "Processed " << counter << " " << counter_name << "." << endl;
            cout << "Time per " << counter_name << ": "
                 << ((sec_accum + (usec_accum / 1000000.0)) / reset_frequency)
                 << endl;

            if ( log != NULL ) {
                (*log) << text << "," << counter_name << "," << counter << ","
                       << ((sec_accum + (usec_accum / 1000000.0)) / reset_frequency)
                       << endl;
            }

            sec_accum = 0.0;
            usec_accum = 0.0;
        }

        inline void reset_time_span_printing_global(
            const char* counter_name, unsigned long& counter,
            double reset_frequency, struct timeval& global_start,
            double& sec_accum, double& usec_accum,
            const char* text, ofstream* log = NULL)
        {
            struct timeval end;
            gettimeofday(&end, NULL);

            long sec = end.tv_sec - global_start.tv_sec;
            long usec = end.tv_usec - global_start.tv_usec;
            if (usec < 0) { --sec; usec+=1000000; }

            // Print out:
            // # processed, time per tuple since last reset, time spent in total
            cout << "Processed " << counter << " " << counter_name << "." << endl;
            cout << "Time per " << counter_name << ": "
                 << ((sec_accum + (usec_accum / 1000000.0)) / reset_frequency) << " "
                 << (sec + (usec / 1000000.0)) << endl;

            if ( log != NULL ) {
                (*log) << text << "," << counter_name << "," << counter << ","
                       << ((sec_accum + (usec_accum / 1000000.0)) / reset_frequency) << " "
                       << (sec + (usec / 1000000.0)) << endl;
            }

            sec_accum = 0.0;
            usec_accum = 0.0;
        }

        inline void global_time_span(
            const char* counter_name, unsigned long& counter,
            struct timeval& global_start,
            const char *text, ofstream* log = NULL)
        {
            struct timeval end;
            gettimeofday(&end, NULL);

            long sec = end.tv_sec - global_start.tv_sec;
            long usec = end.tv_usec - global_start.tv_usec;
            if (usec < 0) { --sec; usec+=1000000; }

            // Print out:
            // # processed, time spent in total
            cout << "Processed " << counter << " " << counter_name << "." << endl;
            cout << "Total time: " << (sec + (usec / 1000000.0)) << endl;

            if ( log != NULL ) {
                (*log) << text << "," << counter_name << "," << counter << ","
                       << (sec + (usec / 1000000.0)) << endl;
            }
        }
        
        // Workload progress statistics
        unsigned int handler_counter = 0;
        unsigned int handler_frequency = 1000;

        struct ProgressIndicator
        {
            unsigned int counter;
            double app_t;
            double wall_t;
            ProgressIndicator(unsigned int c, double at, double wt)
            {
                counter = c;
                app_t = at;
                wall_t = wt;
            }

            string as_string() const
            {
                ostringstream oss;
                oss << counter << "," << app_t << "," << wall_t;
                return oss.str();
            }
        };

        list<ProgressIndicator> progress_trace;

        void trace_progress(double app_t, struct timeval& wall_t)
        {
            // Update progress stats.
            ++handler_counter;
            if ( (handler_counter % handler_frequency) == 0 ) {
                double wt = wall_t.tv_sec + (wall_t.tv_usec / 1000000.0);
                ProgressIndicator p(handler_counter, app_t, wt);
                progress_trace.push_back(p);
            }
        }

        void output_progress_trace(const char* filename)
        {
            ofstream trace_out(filename);
            list<ProgressIndicator>::const_iterator pt_it = progress_trace.begin();
            list<ProgressIndicator>::const_iterator pt_end = progress_trace.end();
            for (; pt_it != pt_end; ++pt_it)
                trace_out << pt_it->as_string() << endl;
            trace_out.close();
        }
    };
};

#else

namespace DBToaster
{
    namespace Profiler
    {
        #define LOCK_HANDLER_PROFILE
        #define UNLOCK_HANDLER_PROFILE

        #define START_HANDLER_SAMPLE(statsType, handlerId)
        #define END_HANDLER_SAMPLE(statType, handlerId)

        #define START_PROFILE(statsType,codeLoc)
        #define END_PROFILE(statsType,codeLoc)

        #define PROFILER_INITIALIZATION
        #define PROFILER_SERVICE_METHOD_IMPLEMENTATION 
        #define PROFILER_ENGINE_IMPLEMENTATION
    };
};

#endif

#endif
