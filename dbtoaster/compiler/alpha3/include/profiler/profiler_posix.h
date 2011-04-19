// Do not include this file directly.
// TODO: test this on Linux + FreeBSD

#include <time.h>

// Linux/BSD profiling
#define NSEC_PER_SEC    1000000000
#define DEBUG_SAMPLE    1

typedef timespec Timestamp;

Timestamp sampleFreq;
void setSampleFrequency() { sampleFreq.tv_sec = 1; sampleFreq.tv_nsec = 0; }

inline Timestamp now() { Timestamp t; clock_gettime(CLOCK_REALTIME, &t); return t; }

inline Timestamp diff(Timestamp end, Timestamp start) {
    Timestamp temp;
    if ((end.tv_nsec-start.tv_nsec)<0) {
        temp.tv_sec = end.tv_sec-start.tv_sec-1;
        temp.tv_nsec = NSEC_PER_SEC+end.tv_nsec-start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}

// TODO: make robust and optimize.
inline Timestamp ceil(Timestamp x, Timestamp base)
{
    uint64_t xv = x.tv_sec * NSEC_PER_SEC + x.tv_nsec;
    uint64_t xb = base.tv_sec * NSEC_PER_SEC + base.tv_nsec;
    uint64_t y = (xv+(xb-1))/xb;
    Timestamp r;
    r.tv_sec = y / NSEC_PER_SEC;
    r.tv_nsec = y % NSEC_PER_SEC;
}

inline size_t divide(Timestamp x, Timestamp y)
{
    uint64_t xv = x.tv_sec * NSEC_PER_SEC + x.tv_nsec;
    uint64_t yv = y.tv_sec * NSEC_PER_SEC + y.tv_nsec;
    return xv/yv;
}

inline SampleUnits createTimestampSample(const boost::any& x)
{
#ifdef DEBUG_SAMPLE
    // Ensure x casts to correct type.
    try {
        const uint64_t& test_x = any_cast<uint64_t>(x);
    } catch (boost::bad_any_cast e) {
        cerr << "Invalid timestamp sample found in profiler!" << endl;
        exit(1);
    }
#endif

    return x;
}

inline boolean isValid(Timestamp& t) {
    return !((t.tv_sec == 0) && (t.tv_nsec == 0));
}

inline void init(Timestamp& t) {
    t.tv_sec = 0;
    t.tv_nsec = 0;
}

inline Timestamp& operator=(Timestamp& t, Timestamp& t1) {
    t.tv_sec = t1.tv_sec;
    t.tv_nsec = t1.tv_nsec;
    return t;
}

inline bool operator<(const Timestamp& l, const Timestamp& r)
{
    return (l.tv_sec < r.tv_sec) ||
        (l.tv_sec == r.tv_sec && l.tv_nsec < r.tv_nsec);
}

inline void copyTimestampProtocol(const Timestamp& l, Protocol::Timestamp& r)
{
    r.tv_sec = l.tv_sec;
    r.tv_nsec = l.tv_nsec;
}
