#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>

using namespace std;

int main(int argc, char** argv){
  boost::hash<std::string> field_hash;
  string curr_string;
  for(int i = 1; i < argc; i++){
    curr_string = argv[i];
    cout << curr_string << "\t" 
         << static_cast<int>(field_hash(curr_string)) << "\n";  
  }
}