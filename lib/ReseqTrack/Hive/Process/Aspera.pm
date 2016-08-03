package ReseqTrack::Hive::Process::Aspera;

use strict;
use warnings;
use File::Path qw( make_path remove_tree );
use File::Basename qw( dirname basename );
use File::Temp qw( tempdir );
use autodie;

use base ('Bio::EnsEMBL::Hive::RunnableDB::SystemCmd');

sub param_defaults {
  return {
    'use_bash_pipefail' => 1,           # Boolean. When true, the command will be run with "bash -o pipefail -c $cmd". Useful to capture errors in a command that contains pipes
    'ascp_exe'          => 'ascp',
    'ascp_param'        => { k => 2, },
    'download_dir'      => undef,
    'upload_dir'        => undef,
    'trim_path'         => undef,     
    'username'          => undef,
    'aspera_url'        => undef,
  }
}

sub run {
  my $self = shift;
  my $filename     = $self->param_required('filename');
  my $ascp_exe     = $self->param_required('ascp_exe');
  my $username     = $self->param_required('username');
  my $aspera_url   = $self->param_required('aspera_url');
  my $work_dir     = $self->param_required('work_dir');
  my $download_dir = $self->param('download_dir');
  my $upload_dir   = $self->param('upload_dir');
  my $trim_path    = $self->param('trim_path');
  my $ascp_param   = $self->param('ascp_param');

  $filename =~ s{\s}{\\ }g;               ## fix for space in filename

  if ( $ascp_exe =~ /\// ){
    die "$ascp_exe doesn't exist",$/  unless -e $ascp_exe;
    die "$ascp_exe not executable",$/ unless -x $ascp_exe;
  }

  die "requir either download_dir or upload_dir",$/
    if !$download_dir && !$upload_dir;
  
  die "mutually exclusive options:  download_dir or upload_dir",$/
    if $download_dir && $upload_dir;

  my $file_basename = basename($filename);
  $file_basename =~ s{\\\s}{_}g;           ## fix for space in filename
  my $log_dir = tempdir ( $file_basename.'XXXX' , DIR => $work_dir, CLEANUP => 0 );
  
  $$ascp_param{'L'} = $log_dir;            ## log dir path is required
  $$ascp_param{'d'} = undef
    if $upload_dir;                        ## required for creatting directory in remote

  
  my $cmd = $ascp_exe;  
  my $ascp_param_str = _get_hash_to_string( $ascp_param );

  $cmd .= ' '. $ascp_param_str
    if $ascp_param_str;
  
  my $trim_re  = qr/$trim_path/ 
     if $trim_path;

  if ( $download_dir && !$upload_dir ){     ## Aspera download command

    my $dest_path     = dirname $filename;
    $dest_path        =~ s/$trim_re//g
                           if $trim_path;   ## trim destination path
    my $download_path = $download_dir .'/'. $dest_path;
    $download_path    =~ s{//}{/}g;
    make_path( $download_path );            ## preserve the directory structure

    $cmd .= ' ' . $username .'@'. $aspera_url . ':' . $filename . ' ' . $download_path;
  }
  elsif ( !$download_dir && $upload_dir ){  ## Aspera upload command

    die "trim_path is required for file upload",$/
      unless $trim_path;
   
    my $upload_path = dirname $filename;   ## remove filename from upload path
    $upload_path =~ s/$trim_re//g
                      if $trim_path;       ## trim upload path
    $upload_path = $upload_dir.'/'.$upload_path .'/';
    $upload_path =~ s{//}{/}g;

    $cmd .= ' ' . $filename . ' ' . $username .'@'. $aspera_url . ':' . $upload_path 
  }

  $self->param('cmd', $cmd);                ## set cmd
  $self->param('use_bash_pipefail', 1);     ## set pipefail
  $self->SUPER::run();                      ## use SUPER::run()

  _check_log_file( $log_dir );
  remove_tree( $log_dir );                  ## cleanup if file transferred correctly
}

sub _get_hash_to_string {
  my ( $ascp_param ) = @_;
  my $str;

  foreach my $k ( keys %{$ascp_param} ){
    my $v = $$ascp_param{$k} // undef;
    $str .= ' -'. $k;
    $str .= ' '. $v
     if $v;
  }
  return $str;
}

sub _check_log_file {
  my ( $log_dir ) = @_;
  my $log_file = $log_dir . '/aspera-scp-transfer.log'; 
  open my $fh, '<', $log_file;
  while( <$fh> ){
    if ( /LOG - Source file transfers passed\s+:\s+(\d)/ ){
      die "file not transferred correctly",$/
        unless $1 > 0;
    }
  }
  close( $fh );
}


1;
