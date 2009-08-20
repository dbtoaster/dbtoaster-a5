#ifndef DBTOASTER_PROFILER_H
#define DBTOASTER_PROFILER_H

#include <iostream>
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
                    buffer = shared_ptr<ProfileBuffer>(new ProfileBuffer());
                    buffer->push_back(sample);
                    statsProfile[locationKey] = buffer;
                }
                else {
                    buffer->push_back(sample);
                }
            }

            // Synchronizes sample buffers to the same slot in the circular buffer.
            virtual Timestamp synchronizeSamples()
            {
                list<ProfileLocationKey> deletedLocations;
                Timestamp syncPoint = ceil(lastSamplePoint, sampleFreq);

                typename StatisticsProfile::iterator statIt = statsProfile.begin();
                typename StatisticsProfile::iterator statEnd = statsProfile.end();
                for (; statIt != statEnd; ++statIt)
                {
                    const ProfileLocationKey& locationKey = statIt->first;
                    shared_ptr<ProfileBuffer> buffer = statIt->second;

                    SampleStrategyKey sampleKey = keyConverter(locationKey);
                    typename ProfileTimestamps::iterator timestampIt = statsTimestamps.find(sampleKey);

                    if ( timestampIt == statsTimestamps.end() ) {
                        cerr << "No timestamp found for profile location!" << endl;
                        exit(1);
                    }

                    Timestamp bufferPoint = timestampIt->second;
                    Timestamp dist = diff(syncPoint, bufferPoint);
                    ProfileBuffer::size_type steps = divide(dist, sampleFreq);

                    if ( steps >= buffer->size() ) {
                        buffer->clear();
                        deletedLocations.push_back(locationKey);
                    }

                    else {
                        buffer->erase(buffer->begin(), buffer->begin()+steps);
                    }
                }

                list<ProfileLocationKey>::iterator delIt = deletedLocations.begin();
                list<ProfileLocationKey>::iterator delEnd = deletedLocations.end();
                for (; delIt != delEnd; ++delIt)
                    statsProfile.erase(*delIt);

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

                    for (; bufIt != bufEnd; ++bufIt)
                        destBuffer.push_back(createProtocolSample(*bufIt));
                }
            }

            virtual void copyStatisticsProfile(Protocol::StatisticsProfile& dest)
            {
                Timestamp syncPoint = synchronizeSamples();
                copyTimestampProtocol(syncPoint, dest.t);

                StatisticsProfile::const_iterator statIt = statsProfile.begin();
                StatisticsProfile::const_iterator statEnd = statsProfile.end();
                for (; statIt != statEnd; ++statIt)
                {
                    Protocol::ProfileLocation destLocKey =
                        statIt->first.createProtocolProfileLocation();

                    Protocol::ProfileBuffer& destBuffer = dest.profile[destLocKey];
                    copyProfileBuffer(statIt, destBuffer);
                }
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
            if ( !(sampleFrequency < diff(now(), codeLoc##Last)) ) { \
                    codeLoc##Var = now(); \
            }

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

        /*
        // Handler level sampling.
        #define START_HANDLER_SAMPLE(statsType, handlerId)     \
            bool handlerId##Sample = false; \
            HandlerSampler::HandlerSampleKey sampleKey(statsType, handlerId); \
            Timestamp handlerId##Last = queryProfile.getLastUpdate(sampleKey); \
            if ( !(sampleFrequency < diff(now(), handlerId##Last) ) { \
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
        Timestamp sampleFrequency;

        void initializeProfiler()
        {
            SampleFunctions timestampHelpers;
            timestampHelpers.sampleConstructor = &createTimestampSample;
            timestampHelpers.protocolConstructor = &createExecTimeProtocolSample;
            queryProfile.addSampleFunctions("cpu", timestampHelpers);

            // TODO: initialize codeHandlers for HandlerSampleKeyConverter
        }

        // Macros for profiler+profiler service setup from main Thrift service.

        // TODO: define constructors/copiers based on actual Thrift types.
        // TODO: auto generate location handlers.
        // TODO: other profiling types, e.g. memory usage.
        #define PROFILER_INITIALIZATION initializeProfiler();

        #define PROFILER_SERVICE_METHOD_IMPLEMENTATION \
        void getStatisticsProfile(Protocol::StatisticsProfile& dest) \
        { \
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

#endif
