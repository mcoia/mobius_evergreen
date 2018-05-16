#!/usr/bin/perl


use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;


our $xmlfile;
our $tablename;
our $schema;
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $drop;
our $primarykey;
our @columns;
our @allRows;

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"schema=s" => \$schema,
"xmlfile=s" => \$xmlfile,
"tablename=s" => \$tablename,
"drop" => \$drop,
"primarykey" => \$primarykey
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig  pathto_opensrf.xml
--xmlfile  pathtoinputxmlfile.xml
--tablename name of the table to edit in schema
--schema (eg. m_slmpl)
--drop (drop table before insert)
--primarykey (create id column)
\n");

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script --xmlconfig configfilelocation\n";
	exit 0;
}
if(!$logFile)
{
	print "Please specify a log file\n";
	exit;
}
if(!$schema)
{
	print "Please specify a DB schema\n";
	exit;
}
	$log = new Loghandler($logFile);
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		

	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});		
		
		
	stuffData();
	#remove special characters
	for my $i (0..$#columns)
	{
		@columns[$i] =~s/\s/_/g;
		@columns[$i] =~s/"/_/g;
		@columns[$i] =~s/\'/_/g;
		@columns[$i] =~s/\(//g;
		@columns[$i] =~s/\)//g;
		@columns[$i] =~s/\*//g;
	}
	
	#drop the table
	my $query = "DROP TABLE IF EXISTS $schema.$tablename";
	$log->addLine($query) if $drop;
	$dbHandler->update($query) if $drop;
	
	#create the table
	$query = "CREATE TABLE $schema.$tablename (";
    $query.="id serial primary key," if $primarykey;
	$query.=$_." TEXT," for @columns;
	$query=substr($query,0,-1).")";
	$log->addLine($query) if $drop;
	$dbHandler->update($query) if $drop;
	
	
	#insert the data
	$query = "INSERT INTO $schema.$tablename (";
	$query.=$_."," for @columns;
	$query=substr($query,0,-1).")\nVALUES\n";
	my $count = 0;
	
	foreach(@allRows)
	{
		$query.="(";
		my @thisrow = @{$_};
		
		for(@thisrow)
		{
			$_ =~ s/\$$/\$ /g;
			$query.='$$'.$_.'$$,';
		}
		
		$query=substr($query,0,-1)."),\n";
		$count++;
		
		if($count % 500 == 0)
		{
			$query=substr($query,0,-2)."\n";
			print "Inserting ".$count." Rows\n";
			$log->addLine($query);
			$dbHandler->update($query);

			
			$query = "INSERT INTO $schema.$tablename (";
			$query.=$_."," for @columns;
			$query=substr($query,0,-1).")\nVALUES\n";
			
		}
		
	}
	$query=substr($query,0,-2)."\n";
	print "Investigation queries:\n";
	print "select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @columns;
	print "Inserted ".$#allRows." Rows\n";
	$log->addLine($query);
	$dbHandler->update($query);
	
	$log->addLogLine(" ---------------- Script End ---------------- ");
	

sub stuffData()
{
	my $xmlFileReader = new Loghandler($xmlfile);
	my @lines = @{$xmlFileReader->readFile()};
	my $finalXML = '';
	for(@lines)
	{
		my $line = $_;
		$line =~ s/\&/&amp;/g;
		$finalXML.=$line;
	}


	my $root = XML::TreeBuilder->new({ 'NoExpand' => 0, 'ErrorContext' => 0 }); # empty tree
    $root->parse($finalXML);
	
	# Get columns
	my @itemNodes = $root->look_down ( _tag => "metadata");
	@columns =();
	for my $metadata (0 .. @itemNodes - 1) 
	{
		my @titles = $itemNodes[$metadata]->look_down (_tag => "item");
		#print "Item node " . ($metadata + 1) . "\n";
		#print "   ", $_->attr_get_i('name'), "\n" for @titles;
		push(@columns, $_->attr_get_i('name')) for @titles;
	}
	
	#print Dumper(\@columns);
	#Get data for columns
	@itemNodes = $root->look_down ( _tag => "row");
	@allRows = ();
	for my $row (0 .. @itemNodes - 1)
	{
		my @titles = $itemNodes[$row]->look_down (_tag => "value");
		my @thisrow = ();
		#print "Item node " . ($row + 1) . "\n";
		#print "   ", $_->as_text(), "\n" for @titles;
		push(@thisrow, $_->as_text()) for @titles;
		push(@allRows,\@thisrow);
	}
	#print Dumper(\@allRows);
}


sub getDBconnects
{
	my $openilsfile = @_[0];
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($openilsfile);
	my %conf;
	$conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
	$conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
	$conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
	$conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
	$conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
	##print Dumper(\%conf);
	return \%conf;

}

exit;