#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;
use autodie;

my $id_list;
my $index_file;
my $output_index;
my $output_tag_file;
my $keyword          = 'SAMPLE_NAME';
my $file_type_header = 'FILE_TYPE';

GetOptions(  'id_list=s'           => \$id_list,
             'index_file=s'        => \$index_file,
             'output_index_file=s' => \$output_index,
             'keyword=s'           => \$keyword,
             'output_tag_file=s'   => \$output_tag_file, 
             'file_type_header=s'  => \$file_type_header
          );

die `perldoc -t $0`  unless $id_list && $index_file && $output_index;

my ( $index_header, $index_data ) = read_file( $index_file );

my $id_list_hash = get_hash( $id_list );
my $output_tag_hash   = undef;
$output_tag_hash      = get_hash( $output_tag_file ) if $output_tag_file;

open my $ofh, '>', $output_index;

print $ofh join ( "\t", @{ $index_header } ), $/;

LINE:
foreach my $index_line ( @{ $index_data } ){
  my $name = $index_line->{ $keyword };
  my $file_type   = $index_line->{ $file_type_header };

  if ( $output_tag_file ){ 
    next LINE unless exists $$output_tag_hash{ $file_type };
  }

  if ( exists ( $$id_list_hash{ $name } ) ) {    
   my %t_hash = %{ $index_line };
   print $ofh join ( "\t", @t_hash{ @{ $index_header } }), $/;
  }
}
close( $ofh );


sub get_hash {
  my ( $list ) = @_;
  my %id_list_hash;

  open my $fh, '<', $list;
  while( <$fh> ){
    chomp;
    $id_list_hash{ $_ }++;
  }
  close( $fh );
  return \%id_list_hash;
}



sub read_file {
  my ($file) = @_;

  open my $fh, '<', $file;
  my @header;
  my @data;
  while (<$fh>) {
    chomp;
    next if m/^#/;
    my @vals = split "\t", $_;
    if (@header) {
      my %row;
      @row{@header} = @vals;
      push @data, \%row;
    }
    else {
      @header = map { uc($_) } @vals;
    }
  }
  close( $fh );
  return \@header, \@data;
}


=pod

=head1 NAME

index/create_sub_index.pl

=head1 SYNPOSIS

  This script should read a  list of sample names  and create a subset index file for the listed samples.
  It can also take lists of experiments or runs as input for creating subset index file.


=head1 OPTIONS

Standard options
 
   id_list              : List of unique ids to populate sub index
 
   index_file           : Index file
 
   output_index_file    : Name of sub index file
 
   keyword              : Keyword to look for sample name column in the index file (default: SAMPLE_NAME )
  
   output_tag_file      : List of file tye tags to filter output index file 

   file_type_header     : Keyword to look for file type  column in the index file (default: FILE_TYPE)

=head1 Examples

Run it like this for the Blueprint project:

 perl index/create_sub_index.pl   -id_list <sample_list_file> -index_file <index_file> -output_index_file <sub_index_file>

 perl index/create_sub_index.pl   -id_list <sample_list_file> -index_file <index_file> -output_index_file <sub_index_file> -output_tag_file <output_tag> 
 
 =cut

