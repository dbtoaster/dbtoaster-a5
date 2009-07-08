#include <iostream>
#include <fstream>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <list>

#include <tr1/tuple>
#include <boost/tokenizer.hpp>
using namespace std;
using namespace tr1;
using namespace boost;

typedef tokenizer <char_separator<char> > tokeniz;

map<int,int > map3;
map<int,int > map2;
list<tuple<int,int> > B;

struct file_stream
{
    public:
    string delim;
    int num_fields;
    ifstream *input_file;

    file_stream(string file_name, string d, int n): num_fields(n), delim(d) {
        input_file = new ifstream(file_name.c_str());
        if(!(input_file->good()))
            cerr << "Failed to open file " << file_name << endl;
    }

    tuple<bool, tokeniz> read_inputs(int line_num)
    {
        char buf[256];
        char_separator<char> sep(delim.c_str());
        input_file->getline(buf, sizeof(buf));
        string line = buf;
        tokeniz tokens(line, sep);

        if(*buf == '\0')
            return make_tuple(false, tokens);

        if (is_valid_num_tokens(tokens)) {
            cerr<< "Failed to parse record at line " << line_num << endl;
            return make_tuple(false, tokens);
        }
        return make_tuple(true, tokens);
    }

    inline bool is_valid_num_tokens(tokeniz tok)
    {
        int i = 0;
        for (tokeniz::iterator beg = tok.begin(); beg!=tok.end(); ++beg, i ++)
            ;
        return i == num_fields;
    }

    void init_stream()
    {
        int line = 0;
        while(input_file -> good())
        {
            tuple<bool, tokeniz> valid_input = read_inputs(line ++);
            if(!get<0> (valid_input))
                break;
            if(!insert_tuple(get<1>(valid_input)))
                cerr<< "Failed to parse record at line " << line << endl;
        }
    }

    virtual bool insert_tuple(tokeniz t) =0;
};

struct stream_B : public file_stream
{
    stream_B(string filename, string delim, int n)
        : file_stream(filename, delim, n) { }

    bool insert_tuple(tokeniz tok)
    {
        tuple<int,int> r;
        int i = 0;
        for (tokeniz::iterator beg = tok.begin(); beg!=tok.end(); ++beg) {
            switch(i) {
            case 0:
                get<0>(r) = atoi((*beg).c_str());
                break;
            case 1:
                get<1>(r) = atoi((*beg).c_str());
                break;
            }

            i ++;
        }

        B.push_back(r);
        return true;
    }

};

list<file_stream *> initialize(ifstream *config)
{
    char buf[256];
    string line;
    char_separator<char> sep(" ");
    int i;
    list <file_stream *> files;
    file_stream * f_stream;

    while(config->good()) {
        string param[4];
        config->getline(buf, sizeof(buf));
        line = buf;
        tokeniz tok(line, sep);
        i = 0;

        for (tokeniz::iterator beg = tok.begin() ; beg != tok.end(); ++beg, i ++) {
            param[i] = *beg;
        }

        if(param[0] == "B")
            f_stream = new stream_B(param[1], param[2], atoi(param[3].c_str()));

        f_stream->init_stream();
        files.push_back(f_stream);
    }
    return files;
}

int on_insert_B(int P,int V) {
    int var11;
    {
        int var12;
        int var15;
        
        list<tuple<int,int> >::iterator B_it5 = B.begin();
        list<tuple<int,int> >::iterator B_end5 = B.end();
        for(; B_it5 != B_end5; ++B_it5)
        {
            int P2 = get<0>(*B_it5);
            int V2 = get<1>(*B_it5);

            {
                {
                    int var13;
                    int var14;
                    var13 = var13 + V;
                    if(P>P2){
                        var14 = var14 + V;
                    }

                    map2[P2] = map2[P2] + var13 * 0.25 + var14;
                }

                if(map2[P2]<0){
                    var12 = min(var12, P2);
                }

            }

        }
        if(map2.find(P)==map2.end()){
            {
                {
                    int var16;
                    int var17;
                    
                    list<tuple<int,int> >::iterator B_it6 = B.begin();
                    list<tuple<int,int> >::iterator B_end6 = B.end();
                    for(; B_it6 != B_end6; ++B_it6)
                    {
                        int P3 = get<0>(*B_it6);
                        int V3 = get<1>(*B_it6);

                        var16 = var16 + V3;
                    }
                    
                    list<tuple<int,int> >::iterator B_it7 = B.begin();
                    list<tuple<int,int> >::iterator B_end7 = B.end();
                    for(; B_it7 != B_end7; ++B_it7)
                    {
                        int P1 = get<0>(*B_it7);
                        int V1 = get<1>(*B_it7);

                        if(P1>P){
                            var17 = var17 + V1;
                        }

                    }
                    map2[P] = map2[P] + var16 * 0.25 + var17;
                }

                if(map2[P]<0){
                    var15 = min(var15, P);
                }

            }

        }

        var11 = min(var12, var15);
        
        map<int,int >::iterator map3_it8 = map3.begin();
        map<int,int >::iterator map3_end8 = map3.end();
        for(; map3_it8 != map3_end8; ++map3_it8)
        {
            int var20 = map3_it8->first;

            {
                int var18;
                if(P<var20){
                    var18 = var18 + P * V;
                }

                map3[var20] = map3[var20] + var18;
            }

        }
        if(map3.find(var11)==map3.end()){
            {
                int var18;
                if(P<var11){
                    var18 = var18 + P * V;
                }

                map3[var11] = var18;
            }

        }

    }

    return map3[var11];
}

int main(int argc, char* argv[])
{
    ifstream *config;
    string fcon;

    if(argc == 1) fcon = "config";
    else fcon = argv[1];

    config = new ifstream(fcon.c_str());
    if(!config->good()) {
        cerr << "Failed to read config file" << fcon << endl;
        exit(1);
    }

    initialize(config);
    {
        list <tuple<int,int> >::iterator front = B.begin();
        list <tuple<int,int> >::iterator end = B.end();
        for ( ; front != end; ++front) {
            on_insert_B( get<0> (*front), get<1> (*front));
        }
    }
    return 0;
}
