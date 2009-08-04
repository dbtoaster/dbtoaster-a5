#ifndef DBTOASTER_PROFILER_H
#define DBTOASTER_PROFILER_H

typedef int32_t ProfileIdentifier;
typedef timespec SampleUnits;

#if defined (__APPLE__) && defined(__MACH__) && !defined(__powerpc__) && !defined( __ppc__ ) && !defined( __ppc64__ )

    typedef uint64_t ProfileUnits;

    ProfileUnits sampleFreq;
    void setSampleFrequency () { sampleFreq = 0; }

    inline ProfileUnits now() { return mach_absolute_time(); }

    inline ProfileUnits diff(ProfileUnits end, ProfileUnits start) {
        ProfileUnits span = end - start;
        static mach_timebase_info_data_t tbi;
        if ( tbi.denom == 0 ) { (void) mach_timebase_info(&tbi);}
        return (span * tbi.numer / tbi.denom);
    }

    inline SampleUnits createSample(ProfileUnits span) {
        temp.tv_sec = span * 1e-9;
        temp.tv_nsec = span - (temp.tv_sec * 1e9);
        return temp;
    }

    inline boolean isValid(ProfileUnits& t) { return t != 0; }

    inline void init(ProfileUnits& t) { t = 0; }

    inline void assign(ProfileUnits& t, ProfileUnits& t1) { t = t1; }

#elif defined (unix) || defined(__unix) || defined(__unix__)

    typedef timespec ProfileUnits;

    ProfileUnits sampleFreq;
    void setSampleFrequency() { sampleFreq.tv_sec = 1; sampleFreq.tv_nsec = 0; }

    inline ProfileUnits now() { ProfileUnits t; clock_gettime(CLOCK_REALTIME, &t); return t; }

    inline ProfileUnits diff(ProfileUnits end, ProfileUnits start) {
        ProfileUnits temp;
        if ((end.tv_nsec-start.tv_nsec)<0) {
            temp.tv_sec = end.tv_sec-start.tv_sec-1;
            temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
        } else {
            temp.tv_sec = end.tv_sec-start.tv_sec;
            temp.tv_nsec = end.tv_nsec-start.tv_nsec;
        }
        return temp;
    }

    inline SampleUnits createSample(ProfileUnits x) { return x; }

    inline boolean isValid(ProfileUnits& t) {
        return !((t.tv_sec == 0) && (t.tv_nsec == 0));
    }

    inline void init(ProfileUnits& t) {
        t.tv_sec = 0;
        t.tv_nsec = 0;
    }

    inline void assign(ProfileUnits& t, ProfileUnits& t1) {
        t.tv_sec = t1.tv_sec;
        t.tv_nsec = t1.tv_nsec;
    }

    inline bool operator<(const ProfileUnits& l, const ProfileUnits& r)
    {
        return (l.tv_sec < r.tv_sec) ||
            (l.tv_sec == r.tv_sec && l.tv_nsec < r.tv_nsec);
    }

#endif

typedef boost::circular_buffer<SampleUnits> RawBuffer;
typedef pair<ProfileUnits, shared_ptr<RawBuffer> > ProfileBuffer;
typedef map<ProfileIdentifier, ProfileBuffer> Profile;

Profile queryProfile;

#define START_PROFILE(profId)                                                  \
    ProfUnits profId##Var;                                                     \
    init(profId##Var);                                                         \
    if ( !(sampleFrequency < diff(now(), queryProfile[profId##].first)) ) {    \
        profId##Var = now();                                                   \
    }

#define END_PROFILE(profId)                                               \
    if ( isValid(profId##Var) ) {                                         \
        shared_ptr<RawBuffer> buffer = queryProfile[profId##].second;     \
        if ( !buffer ) buffer = shared_ptr<RawBuffer>(new RawBuffer());   \
        ProfUnits end = now();                                            \
        buffer.push_back(createSample(diff(end, profId##Var)));           \
        queryProfile[profId##] = make_pair(end, buffer);                  \
    }
#endif
