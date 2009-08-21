// Do not directly include this file.

#include <mach/mach.h>
#include <mach/mach_time.h>

// OSX Profiling helpers
typedef uint64_t Timestamp;

Timestamp sampleFreq;
//void setSampleFrequency (Timestamp s) { sampleFreq = s; }

inline Timestamp now() { return mach_absolute_time(); }

inline Timestamp diff(Timestamp end, Timestamp start)
{
    Timestamp span = end - start;
    static mach_timebase_info_data_t tbi;
    if ( tbi.denom == 0 ) { (void) mach_timebase_info(&tbi);}
    return (span * tbi.numer / tbi.denom);
}

inline Timestamp ceil(Timestamp x, Timestamp base)
{
    cout << "x: " << x << " base: " << base << endl;
    return base * ((x + (base-1)) / base);
}

inline size_t divide(Timestamp x, Timestamp y)
{
    return x / y;
}

inline SampleUnits createTimestampSample(const boost::any& span)
{
    const Timestamp& spanSample = boost::any_cast<Timestamp>(span);
    timespec temp;
    temp.tv_sec = spanSample * 1e-9;
    temp.tv_nsec = spanSample - (temp.tv_sec * 1e9);
    return boost::any(temp);
}

inline string to_string(SampleUnits& sample) {
    ostringstream oss;
    timespec& s = boost::any_cast<timespec&>(sample);
    oss << s.tv_sec << "." << s.tv_nsec;
    return oss.str();
}

inline bool isValid(Timestamp& t) { return t != 0; }

inline void init(Timestamp& t) { t = 0; }

inline void copyTimestampProtocol(const Timestamp& l, Protocol::Timestamp& r)
{
    r.tv_sec = l * 1e-9;
    r.tv_nsec = l - (r.tv_sec * 1e9);
}
