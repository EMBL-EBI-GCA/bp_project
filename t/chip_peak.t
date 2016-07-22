#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Test::More;
use ReseqTrack::Attribute;

BEGIN {
  use_ok('ReseqTrack::Hive::PipeSeed::ChipPeakCallSeed', qw( _check_exp_type _assign_peak_call_type _read_non_match_input ));
}; 

my $attributes  = [];
my $output_hash = {};
my $flag_val    = undef;
my $alt_input_list = "$Bin/test_alt_input_list";

my $attributeA  = ReseqTrack::Attribute->new( -table_name => 'experiment', -attribute_name => 'EXPERIMENT_TYPO', -attribute_value => 'H3K9/14ac' );
my $attributeB  = ReseqTrack::Attribute->new( -table_name => 'experiment', -attribute_name => 'EXPERIMENT_TYPE', -attribute_value => 'H3K9/14ac' );

push @$attributes, $attributeA;
$flag_val = _check_exp_type( $attributes, 'EXPERIMENT_TYPE', 'Input', $output_hash  );
is($flag_val, 0, 'exp test missing EXPERIMENT_TYPE');


$attributes = ();
push @$attributes, $attributeB;
$flag_val = _check_exp_type( $attributes, 'EXPERIMENT_TYPE', 'Input',$output_hash  );
is($flag_val, 1, 'exp test for EXPERIMENT_TYPE');
is($$output_hash{EXPERIMENT_TYPE}, 'H3K9_14ac', 'exp test for H3K9/14ac');


my $broad = undef;
$broad = _assign_peak_call_type( 'H3K4me3' );
isnt($broad,1,'test for non-broad marks');

$broad = undef;
$broad = _assign_peak_call_type( 'H3K9me3' );
is($broad,1,'test for non-broad marks');

my $input_hash = _read_non_match_input( $alt_input_list );
my $input_file = $$input_hash{EXP1};
is($input_file, 'test_input.bam', 'alt bam test');

done_testing();
