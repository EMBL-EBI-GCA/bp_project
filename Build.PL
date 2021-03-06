#!/usr/bin/env perl
use strict;
use warnings;

use Module::Build::Pluggable ( 'CPANfile' );

my $builder = Module::Build::Pluggable->new(
    module_name => 'BlueprintFileMovePath',
    license     => 'apache',
    dist_author => 'Avik Datta <avikd@ebi.ac.uk>, Ernesto Lowy <ernesto@ebi.ac.uk>',
);
$builder->create_build_script();
