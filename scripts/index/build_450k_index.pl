#!/usr/bin/env perl
use strict;
use warnings;
use autodie;
use Getopt::Long;
use Digest::MD5::File qw( file_md5_hex );
use File::Copy;
use File::Basename;
use File::Find;
use File::Temp;
use Data::Dump qw(dump);
use ReseqTrack::Tools::ERAUtils;

my $output_index     = 'array_data.index';
my $db_upload_file   = 'file_lists_for_db.txt';
my $move_list        = 'move_list.txt';
my $samples_key      = 'Sample name';
my $files_key        = 'Raw data File';
my $instrument       = 'Illumina HumanMethylation 450K';
my $type             = 'IDAT_FILE';
my $data_format      = '450K_IDAT';
my $print_header     = undef;
my $samples_input;
my $data_files_input;
my $array_data_index;
my $run_center;
my $study_id;
my $work_dir;
my $files_dir;
my $output_dir;
my $freeze_date;
my $era_user;
my $era_pass;

GetOptions(  'samples_file=s'     => \$samples_input,
             'data_file=s'        => \$data_files_input,
             'output_index=s'     => \$output_index,
             'samples_key=s'      => \$samples_key,
             'files_key=s'        => \$files_key,
             'run_center=s'       => \$run_center, 
             'study_id=s'         => \$study_id,
             'data_format=s'      => \$data_format,
             'instrument=s'       => \$instrument,
             'type=s'             => \$type,
             'work_dir=s'         => \$work_dir,
             'print_header!'      => \$print_header,
             'era_user=s'         => \$era_user,
             'era_pass=s'         => \$era_pass,
             'db_upload_file=s'   => \$db_upload_file,
             'move_list=s'        => \$move_list,
             'array_data_index=s' => \$array_data_index,
             'files_dir=s'        => \$files_dir,
             'output_dir=s'       => \$output_dir,
             'freeze_date=s'      => \$freeze_date,
          );


die `perldoc -t $0` if !$samples_input || !$data_files_input || !$run_center || !$study_id || 
                       !$work_dir      || !$files_dir        || !$output_dir || !$era_user ||
                       !$era_pass      || !$freeze_date;

my @header_lists = qw/	STUDY_ID		STUDY_NAME		CENTER_NAME
			FIRST_SUBMISSION_DATE	DATA_FORMAT		SAMPLE_NAME
			INSTRUMENT_PLATFORM	DISEASE			MOLECULE
                        DISEASE_ONTOLOGY_URI	BIOMATERIAL_PROVIDER	BIOMATERIAL_TYPE
                        CELL_TYPE		SAMPLE_ONTOLOGY_URI	TYPE
			TISSUE_TYPE		TISSUE_DEPOT		DONOR_ID
			DONOR_AGE		DONOR_HEALTH_STATUS	DONOR_SEX
			FILE			FILE_MD5		FILE_SIZE
		    /;

my @file_input_header = ( 'Sample_Name', 
                          'Platform',
                          'Raw data File',
                        );

my @sample_input_header = ( 'Source_name', 'Sample_Name', 
                            'Characteristics[CELL_TYPE]', 
                            'Characteristics[SCIENTIFIC_NAME]', 
                            'Characteristics[BIOMATERIAL_PROVIDER]',
                            'Characteristics[BIOMATERIAL_TYPE]', 
                            'Characteristics[DONOR_SEX]',
                            'Characteristics[MOLECULE]',
                            'Characteristics[DONOR_ID]', 
                            'Characteristics[DONOR_AGE]',
                            'Characteristics[DISEASE]',
                            'Characteristics[DONOR_HEALTH_STATUS]',
                            'Characteristics[TISSUE_TYPE]',
                            'Characteristics[SAMPLE_ONTOLOGY_URI]',
                            'Characteristics[DISEASE_ONTOLOGY_URI]'
                          );
                             

my @output_string = qw/  SCIENTIFIC_NAME	TISSUE_TYPE
	                 DONOR_ID		CELL_TYPE
 	                 RUN_CENTER		SAMPLE_NAME
		     /;

my @db_output_header = qw/ file md5 type size /;

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn);
$era->dbc->db_handle->{LongReadLen} = 66000;

## Creating temp index file
my $temp_index  = File::Temp->new( TEMPLATE => 'array_index_XXXXXX', DIR => $work_dir );
my $index_fname = $temp_index->filename;
$output_index   = $work_dir .'/'.$output_index;
$output_index   =~ s{//}{}g;
die "$output_index already present" if -e $output_index;

my $index_fh;

if ( $print_header ){
  open $index_fh, '>', $index_fname;
  print $index_fh join ("\t", @header_lists),$/;
}
else {
  die "$array_data_index not found" unless -e $array_data_index;
  copy($array_data_index, $index_fname);
  open $index_fh, '>>', $index_fname;
} 

## Creating temp DB upload file
my $temp_db_file = File::Temp->new( TEMPLATE => 'db_upload_XXXXXX', DIR => $work_dir );
my $db_fname     = $temp_db_file->filename;
$db_upload_file  = $work_dir .'/'.$db_upload_file;
$db_upload_file  =~ s{//}{}g;
die "$db_upload_file already present" if -e $db_upload_file;

open my $db_fh, '>', $db_fname;

## Creating temp move file
my $temp_move_list = File::Temp->new( TEMPLATE => 'move_list_XXXXXX', DIR => $work_dir );
my $move_fname     = $temp_move_list->filename;
$move_list = $work_dir .'/'.$move_list;
$move_list =~ s{//}{}g;
die "$move_list already present" if -e $move_list;

open my $mv_fh, '>', $move_fname;

my %old_path_settings = ( file_input    => $files_dir  );
my %new_path_settings = ( file_output   => $output_dir,
                          RUN_CENTER    => $run_center,
                          freeze_date   => $freeze_date,
                          output_string => \@output_string,
                        );

my $samples_hash = get_samples_hash( $samples_input,  \@sample_input_header, $samples_key );
my $files_hash   = get_data_hash( $data_files_input, \@file_input_header,  $samples_key, $files_key );

foreach my $sample( keys %{ $files_hash } ) {
  die "sample $sample not found in sample list",$/ 
     unless exists $$samples_hash{$sample};
  
  my $sample_entry = $$samples_hash{$sample};

  foreach my $file( @{$$files_hash{$sample}} ){
    ## Get file paths
    my $input_path  = get_old_path( $file, \%old_path_settings );
    my $output_path = get_new_path( $sample_entry, $file, \%new_path_settings);
 
    ## Create move list line
    print $mv_fh $input_path,"\t",$output_path,$/;

    ## Create db entry line
    my %db_entries;
    my $file_md5      = file_md5_hex( $input_path );
    my $file_size     = -s $input_path;
    $db_entries{md5}  = $file_md5;
    $db_entries{type} = $type;
    $db_entries{size} = $file_size;
    $db_entries{file} = $input_path;

    print $db_fh join ("\t", @db_entries{@db_output_header}),$/;

    ## Create index entry line
    my %index_setting = ( sample      => $sample_entry,
                          study_id    => $study_id,
                          run_center  => $run_center,
                          file_path   => $output_path,
                          file_md5    => $file_md5,
                          file_size   => $file_size,
                          type        => $type,
                          data_format => $data_format,
                          instrument  => $instrument,
                        );

    my $index_line = prepare_index( $sample, \@header_lists, \%index_setting, $output_dir, $era );
    print $index_fh join ("\t", @$index_line{@header_lists}),$/;
  }
}

close($index_fh);
move($index_fname,$output_index);  ## writing new index file

close($db_fh);
move($db_fname,$db_upload_file);   ## writing new file list for db upload

close($mv_fh);
move($move_fname, $move_list);     ## writing file move list


sub get_samples_hash {
  my ( $file, $sample_input_header, $key ) = @_;
  $key = uc( $key );
  $key =~ s{\s+}{_}g;

  @$sample_input_header = map{ uc($_) } @$sample_input_header; 
  my %sample_hash;
  my @header;
  my %value_list_index;
  open my $fh,'<', $file;
  while ( <$fh> ) {
    chomp;
    next if /^#/;
    if ( @header ) {
      my @lines = split "\t";
      my %hash;
      @hash{@header} = @lines;
      die "no $key found in sample",$/ 
        unless exists $hash{$key};

      foreach my $sample_attribute( @$sample_input_header ){
        $sample_hash{$hash{$key}}{$sample_attribute} = $hash{$sample_attribute}
                 if exists $hash{$sample_attribute};
      }
    }
    else {
      @header = split "\t";
      @header = map { s{Source name}{Source_name}i;$_ } @header; 
      @header = map { s{Sample Name}{Sample_Name}i;$_ } @header; 
      @header = map { uc($_) } @header;
      @header = map { s{\s+$}{}g;$_ } @header; 
      @header = map { s{^\s+}{}g;$_ } @header; 
      @header = map { s{\s+}{_}g;$_ } @header; 
    }
  }
  return \%sample_hash;
}

sub get_data_hash {
  my ( $data_files_input, $file_input_header, $samples_key, $files_key ) = @_;
  $samples_key = uc($samples_key);
  $samples_key =~ s{\s+}{_}g;
  $files_key   = uc($files_key);
  $files_key   =~ s{\s+}{_}g; 
  my %files_hash;
  my @header;
  open my $fh,'<', $data_files_input;
  while ( <$fh> ) {
    chomp;
    next if /^#/; 

    if ( @header ) {
      my @lines = split "\t";
      my %hash;
      @hash{@header} = @lines;

      die "no $samples_key found in sample",$/
        unless exists $hash{$samples_key};
      die "no $files_key found in sample",$/
        unless exists $hash{$files_key};

      my @file_name_lists = split ";", $hash{$files_key};
      push @{$files_hash{$hash{$samples_key}}}, @file_name_lists;
    }
    else {
      @header = split "\t";
      @header = map { s{Sample Name}{Sample_Name}i;$_ } @header; 
      @header = map { uc($_) } @header;
      @header = map { s{\s+$}{}g;$_ } @header; 
      @header = map { s{^\s+}{}g;$_ } @header;
      @header = map { s{\s+}{_}g;$_ } @header; 
    }
  }
  return \%files_hash;
}

sub get_old_path {
 my ( $file, $old_path_settings ) = @_;

 my $old_path; 
 my ($filename) = fileparse($file);
 $filename =~ s{\.gpg$}{};
 my $input_dir = $old_path_settings->{file_input};
 
 find(sub{$old_path=$File::Find::name if $_ eq $filename }, $input_dir); 
 die "$filename not found in $input_dir",$/
    unless $filename;
  return $old_path;  
}

sub get_new_path {
  my ( $sample_entry, $file, $new_path_settings ) = @_;
  my $output_string = $new_path_settings->{output_string};
  my $file_output   = $new_path_settings->{file_output};
  my $freeze_date   = $new_path_settings->{freeze_date};

  $file =~ s{\.gpg$}{}; 
  my ($filename, $dir, $suffix) = fileparse( $file, qr/\.[^.]*/ );
  $filename =~ s{$suffix$}{};
  $filename .= '.' . $freeze_date . $suffix;
 
  my @path;

  foreach my $attribute( @$output_string ){
    if ( exists ( $$sample_entry{$attribute} )){    
      push @path, $sample_entry->{$attribute};
    }
    elsif ( exists (  $$sample_entry{'CHARACTERISTICS['.$attribute.']'})){
      push @path, $sample_entry->{'CHARACTERISTICS['.$attribute.']'};
    }
    else {
      if ( $attribute eq 'TISSUE_TYPE' ){
        push @path,'NO_TISSUE';
      }
      elsif ( $attribute eq 'CELL_TYPE' ){
        push @path,'NO_CELL_TYPE';
      }
      else {
        die "Not found $attribute",$/ 
           unless exists $$new_path_settings{$attribute};
         push @path, $new_path_settings->{$attribute};
      }
    }
  }
  @path = map { s!//!/!g; $_ }   @path;
  @path = map { s/\s/_/g; $_ }   @path;
  @path = map { s/_\//\//g; $_ } @path;
  @path = map { s/_+/_/g; $_ }   @path;
  @path = map { s/[ ,;()=]/_/g; $_ } @path;
  
  my $output_path = join( '/', $file_output, @path, $filename);
  return $output_path;
}

sub get_study_name {
  my ($era, $study_id) = @_;
  my ( $name, $created );
  my $sth = $era->dbc->prepare("select s.ega_id, sx.study_title, s.first_created from study s,
xmltable( '/STUDY_SET/STUDY' passing s.study_xml
columns
        study_title varchar2(512) path '//STUDY_TITLE'
)sx
where s.ega_id =?");
  
  $sth->execute( $study_id);
  while( my $row=$sth->fetchrow_hashref()){
    $name    = $row->{STUDY_TITLE};
    $created = $row->{FIRST_CREATED};
  }
  return $name, $created;
}

sub prepare_index {
  my ($sample, $header_lists, $index_setting, $strip_path, $era ) = @_;
  my $study_id     = $index_setting->{study_id};
  my $run_center   = $index_setting->{run_center};
  my $file_path    = $index_setting->{file_path};
  my $file_md5     = $index_setting->{file_md5};
  my $file_size    = $index_setting->{file_size};
  my $type         = $index_setting->{type};
  my $sample_entry = $index_setting->{sample};
  my $data_format  = $index_setting->{data_format};
  my $instrument   = $index_setting->{instrument};
  my ( $study_name, $submission_date ) = get_study_name($era, $study_id);
  my %entry_hash;

  foreach my $field (@$header_lists){

    if ( exists ( $sample_entry->{$field} )){
      $entry_hash{$field} = $sample_entry->{$field};
    }
    elsif ( exists (  $sample_entry->{'CHARACTERISTICS['.$field.']'})){
      $entry_hash{$field} = $sample_entry->{'CHARACTERISTICS['.$field.']'};
    }
    elsif ( $field eq 'CENTER_NAME' ){
      $entry_hash{$field} = uc($run_center);
    }
    elsif ( $field eq 'STUDY_ID' ){
      $entry_hash{$field} = $study_id;
    }
    elsif ( $field eq 'STUDY_NAME' ){
      $entry_hash{$field} = $study_name;
    }
    elsif ( $field eq 'TYPE' ){
      $entry_hash{$field} = $type;
    }
    elsif ( $field eq 'FILE' ){
      $file_path =~ s{^$strip_path}{}g;
      $file_path =~ s{^/}{}g;
      $entry_hash{$field} = $file_path;
    }
    elsif ( $field eq 'FILE_MD5' ){
      $entry_hash{$field} = $file_md5;
    }
    elsif ( $field eq 'FILE_SIZE' ){
      $entry_hash{$field} = $file_size;
    }
    elsif ( $field eq 'FIRST_SUBMISSION_DATE' ){
      $entry_hash{$field} = $submission_date;
    }
    elsif ( $field eq 'DATA_FORMAT' ){
      $entry_hash{$field} = $data_format;
    }
    elsif ( $field eq 'INSTRUMENT_PLATFORM' ){
      $entry_hash{$field} = $instrument;
    }
    else {
      $entry_hash{$field} = '-';
    }
  }
  return \%entry_hash;
}


=head1 Description

  Script for building array data index for Blueprint

=head2 Options

  samples_file     : Tab delimited text copy of the 'Samples & Phenotype' worksheet from EGA AF template
  data_file        : Tab delimited text copy of the 'Data files' worksheet from EGA AF template
  output_index     : Name of output index file in work_dir (Default: array_data.index )
  db_upload_file   : Output file for database upload in work_dir (Default: file_lists_for_db.txt )
  move_list        : Output file list for file move in work_dir (Default: move_list.txt )
  samples_key      : Name of sample name field from samples_file (Default: Sample name )
  files_key        : Name of file field from data_file (Default: Raw data File )
  run_center       : Run center name
  study_id         : EGA study id
  data_format      : Data file format (Default: 450K_IDAT )
  instrument       : Instrument name (Default: ILLUMINA 450K )
  type             : File type name for index and DB (Default: IDAT_FILE )
  work_dir         : Work directory path
  print_header     : Turn on header in the output index file
  array_data_index : Existing array data index file path
  files_dir        : Directory for input file
  output_dir       : Destination directory for files
  freeze_date      : Data freeze dat for files
  era_user         : ERAPRO db user name
  era_pass         : ERAPRO db pass 

=head2 Example

  For creating an array data index, run like this
  
      perl build_450k_index.pl -samples_file <samples_file> -data_file <data_file> -run_center <run_center> -study_id <study_id> -print_header \
                               -files_dir <files_dir> -output_dir <output_dir> -freeze_date <freeze_date> -era_user <era_user> -era_pass <era_pass>


  For updating an existing array data index file, run like this

      perl build_450k_index.pl -samples_file <samples_file> -data_file <data_file> -run_center <run_center> -study_id <study_id> -array_data_index <array_data_index> \
                               -files_dir <files_dir> -output_dir <output_dir> -freeze_date <freeze_date> -era_user <era_user> -era_pass <era_pass>
 
=cut
