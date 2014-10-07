use strict;
use warnings;
use autodie;
use DBI;
use Getopt::Long;
use Spreadsheet::WriteExcel;

my $key_string        = 'EXPERIMENT_ID';
my $combined_exp_list = '';
my $fastq_index       = '';
my $aln_index         = '';
my $released_id_list  = '';

my $db_host  = '';
my $db_port  = '';
my $db_user  = '';
my $db_pass  = '';
my $db_name  = '';

my $xls_output_file = 'release_report.xls';
my $workbook = Spreadsheet::WriteExcel->new( $xls_output_file );

GetOptions( "experiment_id_list=s" => \$combined_exp_list,
            "released_exp_list=s"  => \$released_id_list,
            "fastq_index=s"        => \$fastq_index,
            "alignment_index=s"    => \$aln_index,
            "db_pass=s"            => \$db_pass,
            "db_host=s"            => \$db_host,
            "db_port=s"            => \$db_port,
            "db_user=s"            => \$db_user,
            "db_name=s"            => \$db_name,
            "output_file=s"        => \$xls_output_file,
          );

die usage() if ( !$db_pass or !$combined_exp_list or !$released_id_list or !$fastq_index or !$aln_index );

my $release_list_out = 'release_report';
my $chip_qc_report   = 'released_chipseq_qc_report';

my @chip_list_array = ( 'ChIP Input',
                        'H3K4me3',
                        'H3K4me1',
                        'H3K9me3',
                        'H3K27ac',
                        'H3K27me3',
                        'H3K36me3',
                      );

my @experiment_order_list = ( 'Bisulfite-Seq',
                              'DNase-Seq',
                              'RNA-Seq',
                            );

push @experiment_order_list, @chip_list_array, 'H2A.Zac', 'H3K9/14ac';

my $released_list_hash = get_list_hash( $released_id_list );
my $combined_list_hash = get_list_hash( $combined_exp_list );

my $fastq_index_hash   = get_index_hash( $fastq_index, $key_string );
my $aln_index_hash     = get_index_hash( $aln_index, $key_string );

my ( $fastq_mapped_hash, $fasta_exp, $fastq_cell_types, 
     $fastq_chipseq_list, $fastq_donor_info ) = map_from_index( $fastq_index_hash, $combined_list_hash );

my ( $aln_mapped_hash, $aln_exp, $aln_cell_types, 
     $aln_chipseq_list, $aln_donor_info )     = map_from_index( $aln_index_hash, $combined_list_hash );

my %cell_type_list = (%{$fastq_cell_types},%{$aln_cell_types});          ## combining cell type info
my %experiment_list = (%{ $fasta_exp },%{ $aln_exp });                   ## combining experiment list
my %chipseq_list   = (%{$fastq_chipseq_list}, %{$aln_chipseq_list});     ## combining chipseq list   ## fix me
my %donor_info     = (%{$fastq_donor_info}, %{$aln_donor_info});         ## combining donor info

my $read_fail_format = $workbook->add_format();                          ## QC fail label formats
$read_fail_format->set_bg_color('yellow');

my $frip_fail_format = $workbook->add_format();
$frip_fail_format->set_bg_color('orange');

my $both_fail_format = $workbook->add_format();
$both_fail_format->set_bg_color('red');

my %bg_format_hash = ( 'READ_COUNT_FAIL'          => $read_fail_format,
                       'FRIP_FAIL'                => $frip_fail_format,
                       'READ_COUNT_AND_FRIP_FAIL' => $both_fail_format,
                     );
my %db_hash = (  'db_host' => $db_host,
                 'db_port' => $db_port,
                 'db_user' => $db_user,
                 'db_pass' => $db_pass,
                 'db_name' => $db_name,
              );

my $release_list_worksheet = $workbook->add_worksheet( $release_list_out );  
my $chip_qc_worksheet      = $workbook->add_worksheet( $chip_qc_report );

my %map_parameter_hash = ( 'fastq_mapped_hash'     => $fastq_mapped_hash,
                           'aln_mapped_hash'       => $aln_mapped_hash,
                           'cell_type_list'        => \%cell_type_list,            ## cell_type array
                           'donor_info'            => \%donor_info,                ## donor info hash
                           'experiment_order_list' => \@experiment_order_list,     ## ordered list of assays for report
                           'released_list_hash'    => $released_list_hash,         ## list of released exp_ids
                           'bg_format_hash'        => \%bg_format_hash,            ## backgroung colour format 
                           'chip_list_array'       => \@chip_list_array,           ## list of chip-seq exp for FULL_CHIP count
                           'chipseq_list'          => \%chipseq_list,              ## list of chip-seq exp_ids
                           'db_hash'               => \%db_hash,                   ## db credentials
                           'fastq_index_hash'      => $fastq_index_hash,        
                           'aln_index_hash'        => $aln_index_hash,
                         );

my $chip_qc_hash  = create_qc_report( $chip_qc_worksheet, \%map_parameter_hash );

$map_parameter_hash{ 'chip_qc_hash'} = $chip_qc_hash;                              ## chip-seq exp qc status

map_data( $release_list_worksheet, \%map_parameter_hash );

$workbook->close();                                                                ## closing Excel file

sub usage{
  warn "perl $0 --experiment_id_list <list> 
                --db_pass <DB_PASS> 
                --released_exp_list <list>
                --alignment_index <index>
                --fastq_index <index>
                --db_host <host>
                --output_file <file>
 ";
}

sub map_data{
  my ( $worksheet, $map_parameter_hash ) = @_;
  
  my $fastq_mapped_hash     = $$map_parameter_hash{'fastq_mapped_hash'};
  my $aln_mapped_hash       = $$map_parameter_hash{'aln_mapped_hash'};
  my $cell_type_list        = $$map_parameter_hash{'cell_type_list'};
  my $donor_info            = $$map_parameter_hash{'donor_info'};
  my $experiment_order_list = $$map_parameter_hash{'experiment_order_list'};
  my $chip_qc_hash          = $$map_parameter_hash{'chip_qc_hash'};
  my $released_list_hash    = $$map_parameter_hash{'released_list_hash'};
  my $bg_format_hash        = $$map_parameter_hash{'bg_format_hash'};
  my $chip_list_array       = $$map_parameter_hash{'chip_list_array'};

  my $row = 0;
  my $col = 0;

  my @cell_type_array = map {$_}  keys ( %{$cell_type_list} );
  my @donor_array     = map {$_} keys (%{$donor_info});
  
  my @header = qw/ DONOR
                   CELL_TYPE
                   DISEASE
                   TREATMENT
                   BIOMATERIAL_PROVIDER
                   DONOR_SEX 
                 /; 

  push @header, @{$experiment_order_list};
  push @header, qw/ FULL_CHIPSEQ
                    FULL_EPIGENOME
                    IN_PREVIOUS_RELEASE
                    IN_CURRENT_RELEASE_PROPOSAL
                  /;


  $worksheet->write_row( $row, $col, \@header);                ## write header row
  $row++;                                                      ## goto next row for writing

  foreach my $cell_type ( @cell_type_array ) {
    foreach my $donor ( @donor_array ) {
      my $disease            = $$donor_info{ $donor }{'disease'};
      my $donor_id           = $$donor_info{ $donor }{'donor_id'};
      my $treatment          = $$donor_info{ $donor }{'treatment'};
      my $biomaterial_source = $$donor_info{ $donor }{'biomaterial_source'};
      my $donor_sex          = $$donor_info{ $donor }{'donor_sex'};
      my $previous_release   = 0;
      my $full_chip          = 0;
      my $flag               = 0;
     
      my %full_chip_list = map { $_ => 0 } @{$chip_list_array};

      my @print_arr = ( $donor_id, $cell_type, $disease, $treatment, $biomaterial_source, $donor_sex );
 
      my @exp_arr;

      foreach my $experiment ( @{$experiment_order_list} ) {
        my $exp_arr_str;
        my $aln_exists_flag = exists ( $$aln_mapped_hash{ $donor }{ $cell_type }{ $experiment }) ? 1 : 0;
        my $fastq_exists_flag = exists ( $$fastq_mapped_hash{ $donor }{ $cell_type }{ $experiment }) ? 1 : 0;
         
        if ( $aln_exists_flag == 1 && $fastq_exists_flag == 1 ) {                           ## when a cell_type / donor present in both file
        
          my @aln_exp_array = @{$$aln_mapped_hash{ $donor }{ $cell_type }{ $experiment }};
          my @fastq_exp_array = @{$$fastq_mapped_hash{ $donor }{ $cell_type }{ $experiment }};

          my @merged_exp_array;
          push @merged_exp_array, @aln_exp_array, @fastq_exp_array;  
          my %exp_uniq_hash = map { $_ => 1 } @merged_exp_array;
          @merged_exp_array = map {$_} keys %exp_uniq_hash;                                        ## removing duplicate experiment ids

          $exp_arr_str = join ";", @merged_exp_array;                                               
          $previous_release += check_in_list( \@merged_exp_array, $released_list_hash  );          ## check for released status
          $full_chip_list{ $experiment }++ if exists $full_chip_list{ $experiment };               ## check for full chip count
          $flag++;
        }
        elsif ( $aln_exists_flag == 1 && $fastq_exists_flag == 0 ) {
          my @aln_exp_array = @{$$aln_mapped_hash{ $donor }{ $cell_type }{ $experiment }};
          $exp_arr_str = join ";", @{$$aln_mapped_hash{ $donor }{ $cell_type }{ $experiment }};  
          $previous_release += check_in_list( \@aln_exp_array , $released_list_hash  );            ## check for released status
          $full_chip_list{ $experiment }++ if exists $full_chip_list{ $experiment };               ## check for full chip count
          $flag++;
        } 
        elsif ( $aln_exists_flag == 0 && $fastq_exists_flag == 1 ) {
          my @fastq_exp_array = @{$$fastq_mapped_hash{ $donor }{ $cell_type }{ $experiment }};
          $exp_arr_str= join ";", @{$$fastq_mapped_hash{ $donor }{ $cell_type }{ $experiment }}; 
          $previous_release += check_in_list( \@fastq_exp_array , $released_list_hash  );          ## check for released status
          $full_chip_list{ $experiment }++ if exists $full_chip_list{ $experiment };               ## check for full chip count
          $flag++;
         }
         elsif ( $aln_exists_flag == 0 && $fastq_exists_flag == 0 ) {
          $exp_arr_str = '.';                                         ## missing experiment, use '.'
         } 
          push @exp_arr, $exp_arr_str; 
      }
      
      $full_chip = get_full_chip_count( \%full_chip_list );

      if ( $flag ) {                                                  ## skip if no experiment is present
        $previous_release = ( $previous_release > 1 ) ? 1 : 0;
        push @print_arr, @exp_arr, $full_chip, '', $previous_release, '';

        foreach my $field ( 0..$#print_arr ) {                        ## for each experiments of report
          my @field_array = split /;/, $print_arr[ $field ];          ## get all experiments, joined by ';'
          
          my $bg_format;                                              ## set background format as undef
          my @new_print_arr;

          foreach my $report_field ( @field_array ) {                 ## for each experiments
            if ( exists ( $$chip_qc_hash{ $report_field } ) ){
              my $qc_status = $$chip_qc_hash{ $report_field };        ## check qc status
 
              $bg_format = $$bg_format_hash{ $qc_status } or die "unknown QC status: $qc_status";
              push @new_print_arr, 'QC_FAIL:' . $report_field;
            }
            else {
              push @new_print_arr, $report_field;
            }
          }
  
          if ( $bg_format ) { 
            my $exp_string;

             warn "All experiments are not qc failed, manual check required:  ", 
                   join ";", @field_array if ( scalar  @field_array > 1 );           ## doesn't work well for concatinated samples   

            $exp_string = join ";", @new_print_arr;
            $worksheet->write( $row, $field, $exp_string , $bg_format );             ## changing bg color
          }
          else {
            $worksheet->write( $row, $field, $print_arr[ $field ]  );  
          }
        }
        $row++;                                                        ## goto next row for writing
      }
    }
  }
}

sub create_qc_report {
  my ( $worksheet, $map_parameter_hash ) = @_;
  
  my $chipseq_list       = $$map_parameter_hash{'chipseq_list'};
  my $db_hash            = $$map_parameter_hash{'db_hash'};
  my $released_list_hash = $$map_parameter_hash{'released_list_hash'};
  my $bg_format_hash     = $$map_parameter_hash{'bg_format_hash'};
  my $fastq_index_hash   = $$map_parameter_hash{'fastq_index_hash'};
  my $aln_index_hash     = $$map_parameter_hash{'aln_index_hash'};

  my $row_count  = 0;
  my $col        = 0;
  my $db_limit   = 900;                                       ## db query limit
 
  my $db_host = $$db_hash{'db_host'};
  my $db_port = $$db_hash{'db_port'};
  my $db_name = $$db_hash{'db_name'};
  my $db_user = $$db_hash{'db_user'};
  my $db_pass = $$db_hash{'db_pass'};
  
  my $dsn = "DBI:mysql:database=$db_name;host=$db_host;port=$db_port";
  my $dbh = DBI->connect($dsn, $db_user, $db_pass, {RaiseError => 1});
  
  my %chip_qc_hash;
  my @exp_header;
  
  my $db_str_hash = get_exp_subset( $chipseq_list, $db_limit ); ## get subset of exprement_ids, limit 900
  
  my $query = ("select  r.*, p.nsc, p.rsc, p.ppqt_quality_tag from  chip_read_counts r join chip_ppqt_metrics_view p on p.name = r.experiment_id and r.experiment_id in  ");

  foreach my $chip_exp_subset ( keys %{ $db_str_hash }){
    $query .= '(' . $chip_exp_subset .')';
    my $sth = $dbh->prepare( $query ) or die "Couldn't prepare statement: " . $dbh->errstr;
    $sth->execute( ) or die "couldn't run execute: " . $sth->errstr;
    die "No rows matched" if $sth->rows == 0;

    unless ( @exp_header ){
      @exp_header = @{ $sth->{NAME} };
      my @header;
      push @header, @exp_header, 'QC_STATUS', 'CELL_TYPE', 'DISEASE';
      $worksheet->write_row( $row_count, $col, \@header); 
      $row_count++;  ## goto next row for writing
    }

    while ( my $row = $sth->fetchrow_hashref() ) {
      my @row_vals = ();
      my $experiment_id = $$row{'experiment_id'} or die "experiment_id missing";

      my $cell_type = assign_value( $experiment_id, 'CELL_TYPE', $fastq_index_hash, $aln_index_hash );
      my $disease   = assign_value( $experiment_id, 'DISEASE', $fastq_index_hash, $aln_index_hash );

      foreach my $db_fields ( @exp_header ){
        my $field_val = $$row{ $db_fields } ? $$row{ $db_fields } : '';
        push @row_vals, $field_val; 
      }

      if ( $$row{'BP_QC_v2_reads'} eq "FAIL" && $$row{'BP_QC_v2_frip'} eq 'FAIL' ) {
        push @row_vals, 'FAIL',$cell_type, $disease;
        $chip_qc_hash{ $experiment_id } = 'READ_COUNT_AND_FRIP_FAIL';
        $both_fail_format = $$bg_format_hash{ 'READ_COUNT_AND_FRIP_FAIL'} or die "Format not found";
        $worksheet->write_row( $row_count, $col, \@row_vals, $both_fail_format);
        $row_count++;
      }
      elsif ( $$row{'BP_QC_v2_reads'} eq "FAIL" && $$row{'BP_QC_v2_frip'} ne 'FAIL' ) {
        push @row_vals, 'FAIL',$cell_type, $disease;
        $chip_qc_hash{ $experiment_id } = 'READ_COUNT_FAIL';   
        $read_fail_format = $$bg_format_hash{ 'READ_COUNT_FAIL' } or die "Format not found";
        $worksheet->write_row( $row_count, $col, \@row_vals, $read_fail_format);
        $row_count++;
      }
      elsif ( $$row{'BP_QC_v2_reads'} ne "FAIL" && $$row{'BP_QC_v2_frip'} eq 'FAIL' ) {
        push @row_vals, 'FAIL',$cell_type, $disease;
        $chip_qc_hash{ $experiment_id } = 'FRIP_FAIL';
        $frip_fail_format = $$bg_format_hash{ 'FRIP_FAIL' } or die "Format not found";
        $worksheet->write_row( $row_count, $col, \@row_vals, $frip_fail_format);
        $row_count++;
      }
      else{
        if ( @row_vals ){
          push @row_vals, 'PASS',$cell_type, $disease;
          $worksheet->write_row( $row_count, $col, \@row_vals);
          $row_count++;
        }
      }
    }
    $sth->finish;
  }
  $dbh->disconnect();
  return \%chip_qc_hash;
}

sub assign_value {
  my ( $experiment_id, $keyword, $fastq_index_hash, $aln_index_hash ) = @_;

  if ( $keyword eq 'CELL_TYPE'){
    $keyword = 'CELL_LINE'   if $$fastq_index_hash{ $experiment_id } && 
                                $$fastq_index_hash{ $experiment_id }{'BIOMATERIAL_TYPE'} eq 'Cell Line';

    $keyword = 'TISSUE_TYPE' if $$fastq_index_hash{ $experiment_id } && 
                                $$fastq_index_hash{ $experiment_id }{'BIOMATERIAL_TYPE'} eq 'Primary Tissue';

    $keyword = 'CELL_LINE'   if $$aln_index_hash{ $experiment_id } && 
                                $$aln_index_hash{ $experiment_id }{'BIOMATERIAL_TYPE'}   eq 'Cell Line'; 

    $keyword = 'TISSUE_TYPE' if $$aln_index_hash{ $experiment_id } && 
                                $$aln_index_hash{ $experiment_id }{'BIOMATERIAL_TYPE'}   eq 'Primary Tissue';
  }

  my $key_term;
  $key_term = $$fastq_index_hash{ $experiment_id }{ $keyword };
  $key_term = $$aln_index_hash{ $experiment_id }{ $keyword };
  die "$keyword not found for $experiment_id" unless $key_term;

  return $key_term;
}

sub get_exp_subset{
  my ( $chipseq_list, $db_limit ) = @_;
  my $count = 0;
  my %string_hash;
  my @exp_id_array;

  foreach my $exp_id ( keys %{$chipseq_list}){
    $count++;
    push @exp_id_array, $exp_id;

    if ( $count == $db_limit ){
      @exp_id_array = map {"'$_'"} @exp_id_array;
      my $string    = join ',', @exp_id_array; 
      $string_hash{ $string }++;      
      $count = 1;
      @exp_id_array = ();
    }
  }

  if ( $count < $db_limit ){                             ## if string length is smaller than db_limit
    @exp_id_array = map {"'$_'"} @exp_id_array;
    my $string    = join ',', @exp_id_array;
    $string_hash{ $string }++;   
  } 
  return \%string_hash;
}

sub check_in_list{
  my ( $exp_array , $released_list_hash  ) = @_;
  my $count = 0;
  foreach my $exp_id ( @{ $exp_array }) {
    $count++ if exists $$released_list_hash{ $exp_id };
  }
  return $count;
}

sub get_full_chip_count{
  my ( $full_chip_list ) = @_;
  my $count = 1;                                       ## assuming its full chip
  
  foreach my $exp_type ( keys %{$full_chip_list}){
    $count = 0 if $$full_chip_list{ $exp_type } == 0;  ## reset count if any missing chip exp found
  }
  return $count;
}

sub map_from_index {
  my ( $index_hash, $list_hash ) = @_;
  
  my %mapped_hash;
  my %experiment_list;
  my %cell_type_list;
  my %chipseq_list;
  my %donor_info;

  foreach my $exp_id ( keys %{ $list_hash } ) {
    #die "exp id: $exp_id not present in index file" unless exists $$index_hash{ $exp_id };
    next unless exists $$index_hash{ $exp_id };                ## skip if exp_id is not present in index hash  
    my $donor_id = $$index_hash{ $exp_id }{'SAMPLE_DESC_2'};
    $donor_id =~ s/\s+/_/g;

    my $exp_type           = $$index_hash{ $exp_id }{'EXPERIMENT_TYPE'};
    my $biomaterial_type   = $$index_hash{ $exp_id }{'BIOMATERIAL_TYPE'};
    my $biomaterial_source = $$index_hash{ $exp_id }{'BIOMATERIAL_PROVIDER'};
    my $donor_sex          = $$index_hash{ $exp_id }{'DONOR_SEX'};
    my $tissue_type        = $$index_hash{ $exp_id }{'TISSUE_TYPE'};
    my $lib_strategy       = $$index_hash{ $exp_id }{'LIBRARY_STRATEGY'};
    my $treatment          = $$index_hash{ $exp_id }{'TREATMENT'};
    $treatment =~ s/\s+/_/g;

    my $cell_type = $$index_hash{ $exp_id }{'CELL_TYPE'};
    $cell_type    = 'Cell_Line' if ( $biomaterial_type eq 'Cell Line' );
    $cell_type    = $tissue_type if ( $biomaterial_type eq 'Primary Tissue' );
    $cell_type    =~ s/\s+/_/g;

    my $keyword;
    if ( $treatment =~ /^-$/ ) {
      $keyword = $donor_id .'_'. $cell_type ;
    }
    else {
      $keyword = $donor_id .'_'. $cell_type . '_'. $treatment;
    }

    $exp_type = 'RNA-Seq' if ( $exp_type eq 'Ribo Minus RNA sequencing' or
                               $exp_type eq 'mRNA-seq' or
                               $exp_type eq 'flRNA-seq' );
  
    $exp_type = 'Bisulfite-Seq' if $exp_type eq 'DNA Methylation';
    $exp_type = 'DNase-Seq'     if $exp_type eq 'Chromatin Accessibility'; 
    $exp_type = 'ChIP Input'    if $exp_type eq 'Input';
    
    $chipseq_list{ $exp_id }++ if $lib_strategy eq 'ChIP-Seq';

    $experiment_list{ $exp_type }++;
    $cell_type_list{ $cell_type }++;
    
    $donor_info{ $keyword }{ 'donor_id' } = $donor_id;
    $donor_info{ $keyword }{ 'treatment' } = $treatment;
    $donor_info{ $keyword }{ 'biomaterial_source' } = $biomaterial_source; 
    $donor_info{ $keyword }{ 'donor_sex' } = $donor_sex;
    $donor_info{ $keyword }{ 'disease' } = $$index_hash{ $exp_id }{'DISEASE'};
 
    push (@{$mapped_hash{ $keyword }{ $cell_type }{ $exp_type }}, $exp_id );        ## cell_type is added for sorting output via cell_type
  }
  return \%mapped_hash, \%experiment_list, \%cell_type_list, \%chipseq_list,\%donor_info;
}

sub get_list_hash {
  my ( $file ) = @_;
  my %hash;

  open my $fh, '<', $file;
  while(<$fh>){
    chomp;
    $hash{$_}++;
  }
  return \%hash;
  close($fh);
}

sub get_index_hash {
my ( $file, $key_string ) = @_;
  open my $fh, '<', $file;
  my @header;
  my %data;
  my $key_index = undef;

  while ( <$fh> ) {
    chomp;
    next if m/^#/;
    my @vals = split "\t", $_;

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
  return \%data;
  close( $fh );
}
