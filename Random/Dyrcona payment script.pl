#!/usr/bin/perl

# A program meant to stress the server and try to make it crash in
# bill retrieval or payment.

use strict;
use warnings;

use OILS;
use Data::Dumper;

bootstrap;
loadIDL;

my %session = login($ARGV[0], $ARGV[1], $ARGV[2]);

#print Dumper %session;

my $query = {
             'select' => {"mbts" => [ 'usr',
                                      {'column' => 'balance_owed', 'transform' => 'sum', 'alias' => 'balance', 'aggregate' => 'true'}
                                    ] },
             'from' => 'mbts',
             'where' => {'xact_finish' => undef},
             'having' => { 'balance_owed' => { '>' => { 'transform' => 'sum', 'value' => 0}}}
            };

my $request = OpenSRF::AppSession->create('open-ils.cstore')
    ->request('open-ils.cstore.json_query', $query);

while (my $result = $request->recv) {
    my $data = $result->{content};
    my $user = flesh_user($data->{usr});
    if (!defined($user) || ref($user) eq 'HASH') {
        print Dumper $user;
        next;
    }
    my $id = ($user->card) ? $user->card->barcode : $user->id;
    printf("%d : %.2f\n", $id, $data->{balance});
    my $bills = OpenSRF::AppSession->create('open-ils.actor')
        ->request('open-ils.actor.user.transactions.history.have_balance', $session{authtoken}, $data->{usr})
            ->gather(1);
    if (ref($bills) eq 'ARRAY') {
        my @payments = ();
        foreach my $bill (@$bills) {
            printf("\t%d : %.2f\n", $bill->id, $bill->balance_owed);
            push(@payments, [$bill->id, $bill->balance_owed]) if ($bill->balance_owed > 0.0);
        }
        if (@payments) {
            my $r = pay_bills($user, \@payments);
            print Dumper $r;
        }
    }
    else {
        print Dumper $bills;
    }
}
$request->finish;

logout();


sub flesh_user
{
    my $id = shift;
    my $response = OpenSRF::AppSession->create('open-ils.actor')
        ->request('open-ils.actor.user.fleshed.retrieve', $session{'authtoken'}, $id,
                   [ 'card' ])
        ->gather(1);
    return $response;
}

sub pay_bills {
    my $user = shift;
    my $paymentref = shift;

    my $result = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.money.payment', $session{'authtoken'},
                         { payment_type => "cash_payment", userid => $user->id, note => "For great justice",
                           payments => $paymentref}, $user->last_xact_id)->gather(1);
    return $result;
}