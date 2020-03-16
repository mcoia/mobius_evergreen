#!/usr/bin/perl
#
# EShandler.pm
#
# Blake Graham-Henderson 
# MOBIUS
# blake@mobiusconsortium.org
# 2019-09-03


package EShandler;
 use Data::Dumper;
 use DateTime;
 use Encode;
 use utf8;
 use Search::Elasticsearch;
 
 our %feedback = ();
 our $type = 0;
 
sub new
{
    my $class = shift;
    my $self = 
    {
        _server => shift,
        _index => shift,
        _type => shift,
        _e => 0,
        _connected => 0
        
    };

    bless $self, $class;

    connectService($self);

    return $self;
}

sub setType
{
    my ($self) = shift;
    $self{'_type'} = shift;
}

sub setIndex
{
    my ($self) = shift;
    $self{'_index'} = shift;
    connectService($self);
}

sub createIndex
{
    my ($self) = shift;
    my %data = shift;
    my $type = $data{'type'} || $self{'_type'};
    my $data = $data{'data'};
    my $bulk = $data{'bulk'} || 0;
    %feedback = ();
    
    if( $self{'_connected'} )
    {
        if( !$bulk)
        {
            $self{'_e'}->index(
                index => $self{'_index'},
                type => $type,
                body => $data
            );
        }
        else
        {
            ## implementing bulk later
        }
    }

}

sub getIndexByID
{
    my ($self) = shift;
    my $type = $data{'type'} || $self{'_type'};    
    my $id = shift;
    if( $self{'_connected'} )
    {
        return $self{'_e'}->get(
            index => $self{'_index'},
            type => $type,
            id => $id
        );
    }
    return 0;
}

sub connectService
{
    my ($self) = shift;
    undef $self{'_e'};
    ## Must have an index defined for this object
    if( $self{'_index'} )
    {
        if( !$self->{'_server'} ) ## default localhost:9200
        {
            # If we are local, we can sniff for the cluster nodes
             $self->{'_e'} = Search::Elasticsearch->new(cxn_pool => 'Sniff');
        }
        elsif( ref($self->{'_server'}) eq 'ARRAY' )
        {
            # server definitinos need to be passed as array if we are remote
            $self->{'_e'} = Search::Elasticsearch->new(
                nodes    => $self->{'_server'}
                );
        }
        else
        {
            $self->{'_e'} = Search::Elasticsearch->new(
                nodes    => $self->{'_server'},
                cxn_pool => 'Sniff'
                );
        }
        $self{'_connected'} = 1;
    }
    print ref $self{'_e'};
}


sub DESTROY
{
    my ($self) = @_[0];
    undef $self->{_e};
    undef $self;
}


1;

