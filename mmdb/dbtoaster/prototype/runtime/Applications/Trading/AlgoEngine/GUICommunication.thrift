#!/home/anton/software/thrift/bin -gen java
#

namespace cpp DBToaster.GuiData
namespace java DBToaster.GuiData

typedef i32 MyInt

service GuiData {

	MyInt getAsksDiff(),
	MyInt getBidsDiff(),
	double getPrice(),
	double getMeanPrice(),
	double getVariance(),
	MyInt getAmountStocks(),
	MyInt getMoney(),

}
