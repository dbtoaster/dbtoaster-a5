#include <iostream>
#include <fstream>
#include <vector>
#include <boost/math/common_factor.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

using namespace std;
using namespace boost;
using namespace boost::filesystem;

void boost_test()
{
  cout << "GCD: " << boost::math::gcd(25,10) << endl;

  path test_path("query_test.cpp");
  if ( exists(test_path) )
    cout << "Found source code in current dir." << endl;
  else
    cout << "Failed to find source code for algo!" << endl;

  return;
}

void stl_test()
{
  vector<int> r;
  r.push_back(1);
  r.push_back(2);

  cout << "Vector size: " << r.size() << endl;

  ofstream out("test.txt");
  out << "Vector size: " << r.size() << endl;
  out.close();

  return;
}

void int_arg_test(int p, int v)
{
  cout << "Int arg test: " << p << ", " << v << endl;
}

void float_arg_test(float p, float v)
{
  cout << "Float arg test: " << p << ", " << v << endl;
}

void double_arg_test(double p, double v)
{
  cout << "Double arg test: " << p << ", " << v << endl;
}

int int_rv_test()
{
  int r = 100;
  cout << "Int rv test: " << r << endl;
  return r;
}

float float_rv_test()
{
  float r = 100.0;
  cout << "Float rv test: " << r << endl;
  return r;
}

double double_rv_test()
{
  double r = 100.0;
  cout << "Double rv test: " << r << endl;
  return r;
}
