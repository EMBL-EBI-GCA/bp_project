#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Test::More;
use ReseqTrack::History;
use ReseqTrack::Attribute;
use ReseqTrack::DBSQL::DBAdaptor;
use ReseqTrack::DBSQL::BaseAdaptor;
use ReseqTrack::DBSQL::DBConnection;
use ReseqTrack::Experiment;

BEGIN {
  use_ok('ReseqTrack::Hive::PipeSeed::ChipPeakCallSeed', qw( _check_exp_type ));
}; 

my $dbc        = ReseqTrack::DBSQL::DBConnection->new( dbname=> 'test', user => 'test');
my $db         = ReseqTrack::DBSQL::DBAdaptor->new( $dbc );
my $adaptor    = ReseqTrack::DBSQL::BaseAdaptor->new( $db );
my $history    = ReseqTrack::History->new(  -adaptor => $adaptor, -table_name =>'experiment' , -other_id => 1 , -comment => 'exp_att' );
my $attributeA = ReseqTrack::Attribute->new( -adaptor => $adaptor, -dbID => 1 , -table_name => 'experiment', -attribute_name => 'EXPERIMENT_TYPO', -attribute_value => 'H3K9/14ac' );
my $attributeB = ReseqTrack::Attribute->new( -adaptor => $adaptor, -dbID => 1 , -table_name => 'experiment', -attribute_name => 'EXPERIMENT_TYPE', -attribute_value => 'H3K9/14ac' );
my $attributes = ();
my $output_hash = {};
my $flag_val;
my @output_experiment_attributes = qw( EXPERIMENT_TYPE );

push @$attributes, $attributeA;
$flag_val = _check_exp_type( $attributes, 'EXPERIMENT_TYPE', 'Input', $output_hash  );
is($flag_val, 0, 'exp test missing EXPERIMENT_TYPE');


$attributes = ();
push @$attributes, $attributeB;
$flag_val = _check_exp_type( $attributes, 'EXPERIMENT_TYPE', 'Input',$output_hash  );
is($flag_val, 1, 'exp test for EXPERIMENT_TYPE');
is($$output_hash{EXPERIMENT_TYPE}, 'H3K9_14ac', 'exp test for H3K9/14ac');

done_testing();
