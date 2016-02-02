#!/usr/bin/perl


use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;

 
my $inputError = 0;
our $xmlfile;
our $schema;

$xmlfile = @ARGV[0];
$schema =  @ARGV[1];

# my $root = XML::TreeBuilder->new ();
# $root->parse ($xmlfile);
my $root = XML::TreeBuilder->new({ 'NoExpand' => 0, 'ErrorContext' => 0 }); # empty tree
    $root->parse_file($xmlfile);


my @itemNodes = $root->look_down ('_tag', 'metadata');
my $nodeCount;

for my $nodeIndex (0 .. @itemNodes - 1) {
    my @titles = $itemNodes[$nodeIndex]->look_down ('_tag', 'value');
   
    print "Item node " . ($nodeIndex + 1) . "\n";
    print "   ", $_->as_text (), "\n" for @titles;
}

exit;