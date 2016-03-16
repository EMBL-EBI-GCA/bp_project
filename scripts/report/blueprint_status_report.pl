#/usr/bin/env perl
use strict;
use warnings;
use autodie;
use DBI;
use Getopt::Long;
use Spreadsheet::WriteExcel;
use Data::Dumper;
use ReseqTrack::Tools::ERAUtils;

my $metadata_tab ;
my $era_user;
my $era_pass;
my $non_ref_samples; # WP10 samples
my $epirr_index;
my $key_string   = 'EXPERIMENT_ID';
my $epirr_dbhost = 'mysql-epirr-rel';
my $epirr_dbport = 4471;
my $epirr_dbname = 'epirr_prod';
my $epirr_dbuser = 'epirr_ro';
my $epirr_dbpass = 'epirr_ro';
my $dbhost;
my $dbport;
my $dbname;
my $dbuser;
my $dbpass;
my $xls_output_file;
my $print_number = undef;

GetOptions( 'metadata_tab=s'    => \$metadata_tab,
            'era_user=s'        => \$era_user,
            'era_pass=s'        => \$era_pass,
            'non_ref_samples=s' => \$non_ref_samples,
            'epirr_index=s'     => \$epirr_index,
            'key_string=s'      => \$key_string,
            'dbhost=s'          => \$dbhost,
            'dbport=s'          => \$dbport,
            'dbname=s'          => \$dbname,
            'dbuser=s'          => \$dbuser,
            'dbpass=s'          => \$dbpass,
            'output=s'          => \$xls_output_file,
            'print_number'      => \$print_number,
          );

my @era_conn = ( $era_user, $era_pass );
my $era = get_erapro_conn(@era_conn); 
$era->dbc->db_handle->{LongReadLen} = 66000;

my $epirr_dsn = "DBI:mysql:database=$epirr_dbname;host=$epirr_dbhost;port=$epirr_dbport";
my $epirr_dbh = DBI->connect( $epirr_dsn, $epirr_dbuser, $epirr_dbpass, {RaiseError => 1});

my $dsn = "DBI:mysql:database=$dbname;host=$dbhost;port=$dbport";
my $dbh = DBI->connect($dsn, $dbuser, $dbpass, {RaiseError => 1});

my @chip_list = ( 'Input',   'H3K4me3',  'H3K4me1', 'H3K9me3',
                  'H3K27ac', 'H3K27me3', 'H3K36me3' );
my @exp_list  = ( 'Bisulfite-Seq', 'RNA-Seq' );
push @exp_list, @chip_list;
my @readcount_list = ( 'Input_mapped_read',   'H3K4me3_mapped_read',  'H3K4me1_mapped_read', 'H3K9me3_mapped_read',
                       'H3K27ac_mapped_read', 'H3K27me3_mapped_read', 'H3K36me3_mapped_read' );

my ( $chip_qc, $chip_read_count )  = get_chip_qc( $dbh );
my ( $data, $index_header )        = read_metadata( $metadata_tab, $key_string );
my $epirr_data                     = read_epirr( $epirr_index );
my $non_ref_list                   = read_list( $non_ref_samples );
my $mapped_data                    = map_data( $data, $index_header, $epirr_data, $non_ref_list, $epirr_dbh );

my %options = ( mapped_data     => $mapped_data, 
                exp_list        => \@exp_list,
                chip_list       => \@chip_list,
                chip_qc         => $chip_qc,
                xls_output      => $xls_output_file,
                readcount_list  => \@readcount_list,
                chip_read_count => $chip_read_count,
                print_number    => $print_number,
              );
write_excel( \%options );

sub write_excel{
  my ( $options ) = @_;
 
  my $mapped_data     = $$options{mapped_data};
  my $exp_list        = $$options{exp_list};
  my $chip_list       = $$options{chip_list};
  my $chip_qc         = $$options{chip_qc};
  my $xls_output_file = $$options{xls_output};
  my $readcount_list  = $$options{readcount_list};
  my $chip_read_count = $$options{chip_read_count};
  my $print_number    = $$options{print_number};

  my $workbook = Spreadsheet::WriteExcel->new( $xls_output_file );
  my $worksheet = $workbook->add_worksheet( 'sample status report' ); 
  my $read_fail_format = $workbook->add_format();                          ## QC fail label formats
  $read_fail_format->set_bg_color('red');
  my $frip_fail_format = $workbook->add_format();
  $frip_fail_format->set_bg_color('magenta');
  my $both_fail_format = $workbook->add_format();
  $both_fail_format->set_bg_color('purple');
  my $ppqt_fail_format = $workbook->add_format();
  $ppqt_fail_format->set_bg_color('yellow');
  my $not_all_fail_format = $workbook->add_format();
  $not_all_fail_format->set_bg_color('lime');

  my %format_hash = ( PPQT_RSC_FAIL            => $ppqt_fail_format,
                      READ_COUNT_FAIL          => $read_fail_format,
                      FRIP_FAIL                => $frip_fail_format,
                      READ_COUNT_AND_FRIP_FAIL => $both_fail_format,
                      NOT_ALL_FAILED           => $not_all_fail_format,
                    );
  my $row = 0;
  my $col = 0;

  my %full_chip_hash = map{ $_ => 1 } @$chip_list;

  my @header = qw/ EPIRR_ID EPIRR_STATUS SAMPLE_GROUP CBR_DONOR_ID DONOR_ID SAMPLE_NAME CELL_TYPE TISSUE_TYPE CELL_LINE DISEASE TREATMENT /;
  push @header, @$exp_list, @$readcount_list;
  push @header, 'CURRENT_EPIGENOME_STATUS', 'FULL_CHIP';
  $worksheet->write_row( $row, $col, \@header); 
  $row++;

  foreach my $key( keys %$mapped_data ){
    my @line;
    exists $$mapped_data{$key}{'EPIRR_ID'} ? push @line,join(";", keys %{$$mapped_data{$key}{'EPIRR_ID'}})
                                           : push @line, "-";
    exists $$mapped_data{$key}{'EPIRR_STATUS'} ? push @line, join(";", keys %{$$mapped_data{$key}{'EPIRR_STATUS'}})
                                               : push @line, "-";
    exists $$mapped_data{$key}{'SAMPLE_GROUP'} ? push @line,$$mapped_data{$key}{'SAMPLE_GROUP'}
                                               : push @line, "-";
    push @line, join(";", keys %{$$mapped_data{$key}{'CBR_DONOR_ID'}});
    push @line, join(";", keys %{$$mapped_data{$key}{'DONOR_ID'}});
    push @line, join(";",keys %{$$mapped_data{$key}{'SAMPLE_NAME'}});
    exists $$mapped_data{$key}{'CELL_TYPE'} ? push @line, join(";", keys %{$$mapped_data{$key}{'CELL_TYPE'}})
                                            : push @line, "-";
    exists $$mapped_data{$key}{'TISSUE_TYPE'} ? push @line, join(";", keys %{$$mapped_data{$key}{'TISSUE_TYPE'}})
                                              : push @line, "-";
    exists $$mapped_data{$key}{'CELL_LINE'} ? push @line,join(";", keys %{$$mapped_data{$key}{'CELL_LINE'}})
                                            : push @line, "-";
    exists $$mapped_data{$key}{'DISEASE'} ? push @line, join(";", keys %{$$mapped_data{$key}{'DISEASE'}})
                                          : push @line, "-";
    exists $$mapped_data{$key}{'TREATMENT'} ? push @line, join(";", keys %{$$mapped_data{$key}{'TREATMENT'}})
                                            : push @line, "-";
    my $full_epigenome_count = 0;
    my $full_chip_count      = 0;
    $col = 0;                                    # set column count
    $worksheet->write_row( $row, $col, \@line);  # write descriptions
    $col = scalar @line;                         # reset column count

    my %chip_read_count_per_exp;
    my @exp_lines;
    foreach my $exp_name ( @$exp_list ){
      my $exp_line;
      my $format = undef;
      my $label        = undef;
      my $pass_count   = undef;
      my  $total_count = undef;
    
      if ( exists ( $$mapped_data{$key}{'EXP'}{$exp_name}) ){
        $exp_line = join(";",@{$$mapped_data{$key}{'EXP'}{$exp_name}});
     
        ( $label, $pass_count, $total_count )  = get_label( $$mapped_data{$key}{'EXP'}{$exp_name}, $chip_qc );
        $format = decide_format( $label, \%format_hash );

        foreach my $exp_id ( @{$$mapped_data{$key}{'EXP'}{$exp_name}} ){
          push @{$chip_read_count_per_exp{$exp_name.'_mapped_read'}}, $$chip_read_count{$exp_id}
               if exists $$chip_read_count{$exp_id};
        }

        if ( $label ){
          $exp_line .= $label; 
          if ( $label =~ /FAIL/){
            $full_epigenome_count++ if $label =~ /NOT_ALL_FAILED/; 
            $full_chip_count++ if $label =~ /NOT_ALL_FAILED/ && exists $full_chip_hash{$exp_name};
          }
          else {
            $full_epigenome_count++;
            $full_chip_count++ if exists $full_chip_hash{$exp_name};
          }
        } 
        else {
          $full_epigenome_count++;
          $full_chip_count++ if exists $full_chip_hash{$exp_name};
        }
      }
      else { 
        $exp_line = '-';
      }
      if ( $print_number ){
        my $count_line = defined $pass_count ? $pass_count.'/'.$total_count: $total_count;
        
        $worksheet->write( $row, $col, $count_line, $format );
      }
      else {
        $worksheet->write( $row, $col, $exp_line, $format );
      }
      $col++;
    }
    
    foreach my $chip_exp_name ( @$readcount_list ){
      my $read_count_line = '-';
      $read_count_line = join (";", @{$chip_read_count_per_exp{$chip_exp_name}})
                         if exists $chip_read_count_per_exp{$chip_exp_name} && 
                                   ref $chip_read_count_per_exp{$chip_exp_name} eq 'ARRAY';
      $worksheet->write( $row, $col, $read_count_line );
      $col++; 
    }

    
    my $current_status = 'Incomplete';
    $current_status = 'Complete'
      if $full_epigenome_count == 9;
    $current_status = '-' 
      if exists $$mapped_data{$key}{'SAMPLE_GROUP'};
    $current_status = '-' 
       if exists $$mapped_data{$key}{'TREATMENT'};
    $worksheet->write( $row, $col, $current_status ); 
    $col++;

    my $full_chip_status = 0;
    $full_chip_status = 1
      if $full_chip_count == 7;
    $full_chip_status = 0
      if exists $$mapped_data{$key}{'SAMPLE_GROUP'};
    $full_chip_status = 0
      if exists $$mapped_data{$key}{'TREATMENT'};
    $worksheet->write( $row, $col, $full_chip_status );

    $row++;
  }
  $workbook->close();
}

sub decide_format {
  my ( $label, $format_hash ) = @_;
  my $format = undef;
  if ( $label && $label ne ''  && $label  !~ /^\s+$/ ){
    if ( $label =~ /NOT_ALL_FAILED/i ){
      $format=$$format_hash{NOT_ALL_FAILED};
    }
    else {
      $label     =~ s/^\s+:\s+//g;
      my @labels = split ":", $label;
      $labels[0] =~ s/\s+//g;
      die "No rules for formating: $label",$/ 
          unless exists $$format_hash{$labels[0]};
      $format=$$format_hash{$labels[0]};
    }
  }
  return $format;
}

sub get_label{
  my ( $exps, $chip_qc ) = @_;
  my $label = undef;
  my $pass_count = 0;
  my $fail_count = 0;
  my $total_count = scalar @$exps;

  foreach my $exp (@$exps){
    next if $exp eq '-';
    next unless exists $$chip_qc{$exp};
    my $chip_qc_label = $$chip_qc{$exp};
    if ( $chip_qc_label eq 'PASS'){
      $pass_count++;
    }
    else {
      $fail_count++;
      $label .= ' : '.$chip_qc_label;
    }
  }
  $label .= ' : NOT_ALL_FAILED'
   if $pass_count > 0 && $fail_count > 0;
  return $label, $pass_count, $total_count;
}

sub get_chip_qc{
  my ( $dbh ) = @_;
  my %chip_qc;
  my %chip_read_count;
  my $sth = $dbh->prepare( "select  * from chip_qc_view" ) or die "Couldn't prepare statement: " . $dbh->errstr;
  $sth->execute( ) or die "couldn't run execute: " . $sth->errstr;
  die "No rows matched" if $sth->rows == 0;

  while ( my $row = $sth->fetchrow_hashref() ) {
    die "No exp id found" unless exists $$row{experiment_source_id};
    my $exp_id     = $$row{experiment_source_id};

    my $read_count = undef;
    $read_count =  $$row{unique_reads_post_filter} 
                   if exists $$row{unique_reads_post_filter};
    $chip_read_count{$exp_id} = $read_count 
                                if $read_count;

    if ( $$row{'BP_QC_v2_reads'} eq "FAIL" && $$row{'BP_QC_v2_frip'} eq 'FAIL' ) {
      $chip_qc{$exp_id} = 'READ_COUNT_AND_FRIP_FAIL';
    }
    elsif ( $$row{'BP_QC_v2_reads'} eq "FAIL" && $$row{'BP_QC_v2_frip'} ne 'FAIL' ) {
      $chip_qc{$exp_id} = 'READ_COUNT_FAIL';
    }
    elsif ( $$row{'BP_QC_v2_reads'} ne "FAIL" && $$row{'BP_QC_v2_frip'} eq 'FAIL' ) {
      $chip_qc{$exp_id} = 'FRIP_FAIL';
    }
    elsif ( $$row{'BP_QC_v2_reads'} ne "FAIL" && $$row{'BP_QC_v2_frip'} ne 'FAIL' && $$row{'BP_QC_v2_RSC'} eq 'CAUTION' ) {
      $chip_qc{$exp_id} = 'PPQT_RSC_FAIL';
    }
    else {
      $chip_qc{$exp_id} = 'PASS';
    }
  }
  return \%chip_qc, \%chip_read_count;
}

sub modify_desc{
  my ( $value ) = @_;
  $value =~ s/\s+/_/g;
  $value =~ s/[ ,;()=]/_/g;
  $value =~ s/_\//\//g;
  $value =~ s/_+/_/g;
  $value = uc( $value );
  return $value;
}

sub map_data{
  my ( $data, $index_header, $epirr_data, $non_ref_list, $epirr_dbh ) = @_;
  my %mapped_data;
  
  foreach my $exp ( keys %{$data} ){
    my $check_ena_status = $$data{$exp}{'EXPERIMENT_STATUS'};
    next unless $check_ena_status eq 'private' or  
            $check_ena_status eq 'public';                  # remove suppressed experiments in EGA
    my $check_sample_status = $$data{$exp}{'SAMPLE_STATUS'};
    next unless $check_sample_status eq 'private' or
            $check_sample_status eq 'public';               # remove suppressed samples in EGA 

    if ( exists ($$epirr_data{$exp} )){
      my $epirr_id = $$epirr_data{$exp};
      $$data{$exp}{'EPIRR_ID'}=$epirr_id;
      my $epirr_status = get_epirr_status( $epirr_id, $epirr_dbh ); 
      $$data{$exp}{'EPIRR_STATUS'}=$epirr_status;
    }
    else {      
      $$data{$exp}{'EPIRR_ID'}='';
      $$data{$exp}{'EPIRR_STATUS'}='';
    }
  
    my $samp_desc_1 = modify_desc($$data{$exp}{'SAMPLE_DESC_1'});
    my $samp_desc_2 = modify_desc($$data{$exp}{'SAMPLE_DESC_2'});
    my $samp_desc_3 = modify_desc($$data{$exp}{'SAMPLE_DESC_3'});
    my $key = $samp_desc_1.'_'.$samp_desc_2.'_'.$samp_desc_3;

    my $exp_type     = $$data{$exp}{'EXPERIMENT_TYPE'};
    my $lib_strategy = $$data{$exp}{'LIBRARY_STRATEGY'};
    next unless $lib_strategy eq 'Bisulfite-Seq' or 
                $lib_strategy eq 'ChIP-Seq' or
                $lib_strategy eq 'RNA-Seq';         # filter library strategy

    push (@{$mapped_data{$key}{'EXP'}{$exp_type}},$exp)
      if $lib_strategy eq 'ChIP-Seq';
    push (@{$mapped_data{$key}{'EXP'}{$lib_strategy}},$exp)
      unless $lib_strategy eq 'ChIP-Seq';
    my $sample_name = $$data{$exp}{'SAMPLE_NAME'};
    $mapped_data{$key}{'SAMPLE_NAME'}{$sample_name}++;

    $mapped_data{$key}{'SAMPLE_GROUP'}='NON_REF'
       if exists $$non_ref_list{$sample_name};
    my $donor_id    = $$data{$exp}{'SAMPLE_DESC_2'};
    $mapped_data{$key}{'DONOR_ID'}{$donor_id}++;

    length($sample_name) == 8 
             ? $mapped_data{$key}{'CBR_DONOR_ID'}{substr($sample_name,0,6)}++
             : $mapped_data{$key}{'CBR_DONOR_ID'}{$donor_id}++;

    my $cell_type   = $$data{$exp}{'CELL_TYPE'};
    $mapped_data{$key}{'CELL_TYPE'}{$cell_type}++
       if $cell_type;
    my $tissue_type = $$data{$exp}{'TISSUE_TYPE'};
    $mapped_data{$key}{'TISSUE_TYPE'}{$tissue_type}++
       if $tissue_type;
    my $cell_line   = $$data{$exp}{'CELL_LINE'};
    $mapped_data{$key}{'CELL_LINE'}{$cell_line}++
       if $cell_line;
    my $treatment   = $$data{$exp}{'TREATMENT'};
    $mapped_data{$key}{'TREATMENT'}{$treatment}++
       if $treatment &&  $treatment !~ /^(NA|NONE|-)$/i ;
    my $disease     = $$data{$exp}{'DISEASE'};
    $mapped_data{$key}{'DISEASE'}{$disease}++
       if $disease ne '-';
    my $epirr_id    = $$data{$exp}{'EPIRR_ID'};
    $mapped_data{$key}{'EPIRR_ID'}{$epirr_id}++
       if $epirr_id;
    my $epirr_status = $$data{$exp}{'EPIRR_STATUS'};
    $mapped_data{$key}{'EPIRR_STATUS'}{$epirr_status}++
       if $epirr_status;
  }

  return \%mapped_data;
}

sub get_epirr_status{
  my ( $epirr_id, $epirr_dbh ) = @_;
  my $status;

  my $sth = $epirr_dbh->prepare("select s.name from dataset d, dataset_version ds, status s
                                 where 
                                 d.dataset_id = ds.dataset_id and
                                 s.status_id = ds.status_id and
                                 ds.is_current = 1 and
                                 d.accession = ?");
  $sth->execute($epirr_id);
  while(my $row=$sth->fetchrow_arrayref()){
    $status = $$row[0];
  }
  return $status;
}

sub read_list{
  my ( $file ) = @_;
  my %list;

  open my $fh, '<', $file;
  while ( <$fh> ) {
    chomp;
    next if m/^#/;
    $list{$_}++;
  }
  close($fh);
  return \%list;
}

sub read_epirr{
  my ( $file ) = @_;
  my ( @header, %epirr_data );

  open my $fh, '<', $file;
  while ( <$fh> ) {
    chomp;
    next if m/^#/;
    my @vals = split "\t";

    if ( @header ) {
      my %row;
      @row{@header} = @vals;
      die unless exists $row{'EXPERIMENT_ID'};
      die unless exists $row{'EPIRR_ID'};
      my $exp_id   = $row{'EXPERIMENT_ID'};
      my $epirr_id = $row{'EPIRR_ID'};
      $epirr_data{$exp_id} = $epirr_id;
    }
    else {
      @header = map { uc($_) } @vals;
    } 
  }
  close($fh);
  return \%epirr_data;
}

sub read_metadata{
  my ( $file, $key_string ) = @_;
  my ( @header, %data );

  open my $fh, '<', $file;
  my $key_index = undef;

  while ( <$fh> ) {
    chomp;
    next if m/^#/;
    my @vals = split "\t";

    if ( @header ) {
      die "$key_string not found in $file\n" unless $key_index >= 0;
      $data { $vals[$key_index] }{ $header[$_] } = $vals[$_] for 0..$#header;
    }
    else {
      @header = map { uc($_) } @vals;
      my @key_index_array = grep{ $header[$_] eq $key_string } 0..$#header;
      $key_index = $key_index_array[0];
    }
  }
  close( $fh );
  return \%data, \@header;
}
