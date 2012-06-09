
int main(int argc, char* argv[]) {
	boost::archive::xml_oarchive oa(cout, 0);

	dbtoaster::Program p(argc, argv);
	dbtoaster::Program::snapshot_t snap;

	cout << "Initializing program" << endl;
	p.init();

	cout << "Running program" << endl;
	if( argc > 1 && !strcmp(argv[1],"async") )
	{
		boost::unique_future<int> fp = p.run_async();
		do
		{
			snap = p.take_snapshot_async();
			oa<<BOOST_SERIALIZATION_NVP_OF_PTR(snap);
		}
		while( !fp.is_ready() );
	}
	else
	{
		p.run();
	}

	snap = p.take_snapshot();
	oa<<BOOST_SERIALIZATION_NVP_OF_PTR(snap);

	return 0;
}
