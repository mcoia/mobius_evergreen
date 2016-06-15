#!/usr/bin/perl

use lib qw(../);
use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;
use REST::Client;
use bignum ( p => -10 );


our $wordnikapikey='';
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $dt;

our @columns;
our @allRows;

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"wordnikapikey=s" => \$wordnikapikey
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
--wordnikapikey key
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

	$log = new Loghandler($logFile);
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		

	my %dbconf = %{getDBconnects($xmlconf)};
	
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
	$log->addLogLine("gathering up 25 words at 4 letters each");
	my @short = @{getWords(1200,4)};
	$log->addLogLine("gathering up 25 words at 7 letters each");
	my @long = @{getWords(1200,7)};
	$log->addLine(Dumper(\@short));
	$log->addLine(Dumper(\@long));
	
	my @shortwordstime;
	foreach(@short)
	{
		clockStart();
		searchQuery($_);
		my %duration = %{clockEnd()};
		my $seconds = $duration{seconds} + ( $duration{minutes} * 60 );
		my $nanoseconds = $duration{nanoseconds};
		$seconds+= ($nanoseconds / 1000000000); #1 billion nanoseconds in 1 second
		# print $seconds."\n";
		push(@shortwordstime, $seconds);
	}
	$log->addLine(Dumper(\@shortwordstime));
	my $average=0;
	foreach(@shortwordstime)
	{
		$average += $_;
	}
	my $average = $average / ($#shortwordstime+1);
	$log->addLine("short average: $average");
	
	my @longwordstime;
	foreach(@long)
	{
		clockStart();
		searchQuery($_);
		my %duration = %{clockEnd()};
		my $seconds = $duration{seconds} + ( $duration{minutes} * 60 );
		my $nanoseconds = $duration{nanoseconds};
		$seconds+= ($nanoseconds / 1000000000); #1 billion nanoseconds in 1 second
		# print $seconds."\n";
		push(@longwordstime, $seconds);
	}
	$log->addLine(Dumper(\@longwordstime));
	my $average=0;
	foreach(@longwordstime)
	{
		$average += $_;
	}
	my $average = $average / ($#longwordstime+1);
	$log->addLine("long average: $average");
	
	
 
 
 #print $client->responseContent(),"\n";
 # print $client->responseHeader('ResponseHeader'),"\n";
 #$client->request('GET','http://api.wordnik.com:80/v4/words.json/randomWords','request body content', \%params);
 #print $client->responseContent(),"\n";
 #print $client->responseHeader('ResponseHeader'),"\n";
	
	$log->addLogLine(" ---------------- Script End ---------------- ");

	
sub searchQuery
{
	my $word = shift;
	my $query = "
	SELECT  *
	          FROM  search.query_parser_fts(
	                    1::INT,
	                    0::INT,
	                    \$core_query_12135\$
	WITH x9ffe220_keyword_xq AS (SELECT 
	      (to_tsquery('english_nostop', COALESCE(NULLIF( '(' || btrim(regexp_replace(search_normalize(split_date_range(\$_12135\$!!!searchword!!!\$_12135\$)),E'(?:\\s+|:)','&','g'),'&|')  || ')', '()'), '')) || to_tsquery('simple', COALESCE(NULLIF( '(' || btrim(regexp_replace(search_normalize(split_date_range(\$_12135\$!!!searchword!!!\$_12135\$)),E'(?:\\s+|:)','&','g'),'&|')  || ')', '()'), ''))) AS tsq,
	      (to_tsquery('english_nostop', COALESCE(NULLIF( '(' || btrim(regexp_replace(search_normalize(split_date_range(\$_12135\$!!!searchword!!!\$_12135\$)),E'(?:\\s+|:)','&','g'),'&|')  || ')', '()'), '')) || to_tsquery('simple', COALESCE(NULLIF( '(' || btrim(regexp_replace(search_normalize(split_date_range(\$_12135\$!!!searchword!!!\$_12135\$)),E'(?:\\s+|:)','&','g'),'&|')  || ')', '()'), ''))) AS tsq_rank ),lang_with AS (SELECT id FROM config.coded_value_map WHERE ctype = 'item_lang' AND code = \$_12135\$eng\$_12135\$)
	SELECT  m.metarecord AS id,
	        ARRAY_AGG(DISTINCT m.source) AS records,
	        1.0/((AVG(
	          (COALESCE(ts_rank_cd('{0.1, 0.2, 0.4, 1.0}', x9ffe220_keyword.index_vector, x9ffe220_keyword.tsq_rank, 14) * x9ffe220_keyword.weight, 0.0))
	        )+1 * COALESCE( NULLIF( FIRST(mrv.vlist @> ARRAY[lang_with.id]), FALSE )::INT * 5, 1)))::NUMERIC AS rel,
	        1.0/((AVG(
	          (COALESCE(ts_rank_cd('{0.1, 0.2, 0.4, 1.0}', x9ffe220_keyword.index_vector, x9ffe220_keyword.tsq_rank, 14) * x9ffe220_keyword.weight, 0.0))
	        )+1 * COALESCE( NULLIF( FIRST(mrv.vlist @> ARRAY[lang_with.id]), FALSE )::INT * 5, 1)))::NUMERIC AS rank, 
	        FIRST(pubdate_t.value) AS tie_break
	  FROM  metabib.metarecord_source_map m
	        
	        LEFT JOIN (
	          SELECT fe.*, fe_weight.weight, x9ffe220_keyword_xq.tsq, x9ffe220_keyword_xq.tsq_rank /* search */
	            FROM  metabib.keyword_field_entry AS fe
	              JOIN config.metabib_field AS fe_weight ON (fe_weight.id = fe.field)
	            JOIN x9ffe220_keyword_xq ON (fe.index_vector @@ x9ffe220_keyword_xq.tsq)
	        ) AS x9ffe220_keyword ON (m.source = x9ffe220_keyword.source)
	        LEFT JOIN metabib.record_sorter pubdate_t ON m.source = pubdate_t.source AND attr = 'pubdate'
	        
	        INNER JOIN metabib.record_attr_vector_list mrv ON m.source = mrv.source
	        
	        ,lang_with
	  WHERE 1=1
	        AND (
	          (x9ffe220_keyword.id IS NOT NULL)
	        )
	  GROUP BY 1
	  ORDER BY 4 ASC NULLS LAST, 5 DESC NULLS LAST, 3 DESC
	  LIMIT 10000
	\$core_query_12135\$::TEXT,
	                    \$\${}\$\$::INT[],
	                    \$\${}\$\$::INT[],
	                    NULL::INT,
	                    1000::INT,
	                    10000::INT,
	                    't'::BOOL,
	                    'f'::BOOL,
	                    'f'::BOOL,
	                    174::INT
	                );
	";
	
	$query =~ s/!!!searchword!!!/$word/g;
	# $log->addLine($query);
	print "\n".$word;
	$dbHandler->query($query);
}

sub getWords
{
	my $quantity = @_[0];
	my $length = @_[1];
	my @words;
	my %params = (
		hasDictionaryDef=> "true",
		includePartOfSpeech=> "noun",
		minDictionaryCount=> 3,
		maxDictionaryCount => -1,
		minCorpusCount=> 10000,
		maxCorpusCount => -1,
		minLength=> 4,
		maxLength=> 12,
		limit => $quantity,
		api_key => "$wordnikapikey"
	 );
	# my $client = REST::Client->new();
	# $client->GET('http://api.wordnik.com:80/v4/words.json/randomWords',\%params);
	my $url='http://api.wordnik.com:80/v4/words.json/randomWords?';
	while ((my $key, my $attr) = each(%params)) { $url.="&$key=$attr";}
	# print "$url\n";

	my $req = HTTP::Request->new( 'GET' => $url );
	my $ua = LWP::UserAgent->new;
	my $response = $ua->request($req);

	my @splitter = split('"word":"',$response->decoded_content);
	shift @splitter;
	foreach(@splitter)
	{
		my @t = split('"',$_);
		my $word = @t[0];
		push(@words,$word);
	}

	return \@words;
}

sub clockStart
{
	$dt = DateTime->now(time_zone => "local");
}

sub clockEnd
{
	my $afterProcess = DateTime->now(time_zone => "local");
	my $difference = $afterProcess - $dt;
	return $difference;
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