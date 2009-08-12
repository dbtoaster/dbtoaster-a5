namespace cpp DBToaster.Profiler.Protocol
namespace java DBToaster.Profiler.Protocol

enum SampleType {
     EXEC_TIME = 1,
     MEM_USAGE = 2
}

struct Timestamp {
    1: i64 tv_sec,
    2: i64 tv_nsec
}

typedef i64 MemUsage

struct SampleUnits {
    1: SampleType sampleType,
    2: optional Timestamp execTime,
    3: optional MemUsage memUsage
}

typedef list<SampleUnits> ProfileBuffer

struct ProfileLocation {
    1: string statName,
    2: i32 codeLocation
}

struct StatisticsProfile {
    1: Timestamp t,
    2: map<ProfileLocation, ProfileBuffer> profile
}

// Note: since Thrift currently does not support easy service
// multiplexing, binaries requiring profiling should inherit
// from the Profiler service to include their own methods along with
// profile retrieval.
// Read THRIFT-66 for more info.
service Profiler {
    StatisticsProfile getStatisticsProfile()
}
