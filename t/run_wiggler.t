#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Test::More;
use ReseqTrack::Attribute;

BEGIN {
  use_ok('ReseqTrack::Hive::PipeSeed::RunWigglerSeed', qw( _check_exp_type ));
}; 

my $attributeA  = ReseqTrack::Attribute->new(  -table_name => 'experiment', -attribute_name => 'EXPERIMENT_TYPO', -attribute_value => 'H3K9/14ac' );
my $attributeB  = ReseqTrack::Attribute->new(  -table_name => 'experiment', -attribute_name => 'EXPERIMENT_TYPE', -attribute_value => 'H3K9/14ac' );
my $attributes  = [];
my $output_hash = {};
my $flag_val;

push @$attributes, $attributeA;
$flag_val = _check_exp_type( $attributes, 'EXPERIMENT_TYPE', $output_hash );
is($flag_val, 0, 'exp test missing EXPERIMENT_TYPE');


$attributes = ();
push @$attributes, $attributeB;
$flag_val = _check_exp_type( $attributes, 'EXPERIMENT_TYPE', $output_hash );
is($flag_val, 1, 'exp test for EXPERIMENT_TYPE');
is($$output_hash{EXPERIMENT_TYPE}, 'H3K9_14ac', 'exp test for H3K9/14ac');

done_testing();
