package ReseqTrack::Hive::PipeConfig::Alignment_run_conf;

use strict;
use warnings;

use base ('ReseqTrack::Hive::PipeConfig::ReseqTrackGeneric_conf');

sub default_options {
    my ($self) = @_;

    return {
        %{ $self->SUPER::default_options() },

        'pipeline_name' => 'align',
        seeding_module => 'ReseqTrack::Hive::PipeSeed::Alignment_run',
        seeding_options => {
            collection_type                => $self->o('type_fastq'),
            output_study_columns           => $self->o('study_columns'),
            output_sample_columns          => $self->o('sample_columns'),
            output_experiment_columns      => $self->o('experiment_columns'),
            output_columns                 => $self->o('run_columns'),
            output_study_attributes        => $self->o('study_attributes'),
            output_sample_attributes       => $self->o('sample_attributes'),
            output_experiment_attributes   => $self->o('experiment_attributes'),
            output_attributes              => $self->o('run_attributes'),
            require_study_columns          => $self->o('require_study_columns'),
            require_sample_columns         => $self->o('require_sample_columns'),
            require_experiment_columns     => $self->o('require_experiment_columns'),
            require_columns                => $self->o('require_run_columns'),
            exclude_study_columns          => $self->o('exclude_study_columns'),
            exclude_sample_columns         => $self->o('exclude_sample_columns'),
            exclude_experiment_columns     => $self->o('exclude_experiment_columns'),
            exclude_columns                => $self->o('exclude_run_columns'),
            require_study_attributes       => $self->o('require_study_attributes'),
            require_sample_attributes      => $self->o('require_sample_attributes'),
            require_experiment_attributes  => $self->o('require_experiment_attributes'),
            require_attributes             => $self->o('require_run_attributes'),
            exclude_study_attributes       => $self->o('exclude_study_attributes'),
            exclude_sample_attributes      => $self->o('exclude_sample_attributes'),
            exclude_experiment_attributes  => $self->o('exclude_experiment_attributes'),
            exclude_attributes             => $self->o('exclude_run_attributes'),
            metadata_file                  => $self->o('metadata_file'),
            path_names_array               => $self->o('path_names_array'),
          },

         regexs               => undef,
         type_fastq           => $self->o('type_fastq'),
         metadata_file        => $self->o('metadata_file'),
         path_names_array     => [ 'sample_desc_1', 'sample_desc_2', 'sample_desc_3', 'library_strategy', 'center_name' ],
        
         species_name         => 'homo_sapiens',
         genome_build         => 'GRCh38',
        
        'chunk_max_reads'     => 5000000,
        'split_exe'           => $self->o('ENV', 'RESEQTRACK').'/c_code/split/split',
        'validate_bam_exe'    => $self->o('ENV', 'RESEQTRACK').'/c_code/validate_bam/validate_bam',
        'bwa_exe'             => '/nfs/1000g-work/ihec/work/bp_pipe/software/bwa-0.7.7/bwa',
        'samtools_exe'        => '/nfs/1000g-work/G1K/work/bin/samtools/samtools',
        'squeeze_exe'         => '/nfs/1000g-work/G1K/work/bin/bamUtil/bin/bam',
        'gatk_dir'            => '/nfs/1000g-work/G1K/work/bin/gatk/dist/',
        'picard_dir'          => '/nfs/1000g-work/G1K/work/bin/picard',
        
        'root_output_dir'     => $self->o('root_output_dir'),
        'dict_file'           => $self->o('dict_file'),
        'reference_uri'       => undef,
        'ref_assembly'        => undef,
        'ref_species'         => undef,
        'header_lines_file'   => undef,
        'reference'           => $self->o('reference'),

        'bam_type'            => $self->o('bam_type'),
        'bai_type'            => $self->o('bai_type'),
        'bas_type'            => $self->o('bas_type'),
        'bwa_algorithm'       => 'backtrack',
        'bwa_read_trimming'   => 5,
        'bwa_options'         => {
                                   algorithm     => $self->o('bwa_algorithm'),
                                   read_trimming => $self->o('bwa_read_trimming'),
                                 },

        'RGSM'                => '#sample_source_id#',
        'RGPU'                => '#run_source_id#',


        'collection_name'     => '#run_source_id#', 
        'build_collection'    => 1,


        'sample_attributes'     => [],
        'sample_columns'        => ['sample_id', 'sample_source_id', 'sample_alias'],
        'run_attributes'        => [],
        'run_columns'           => ['run_id', 'run_source_id', 'center_name', 'run_alias'],
        'study_attributes'      => [],
        'study_columns'         => ['study_source_id'],
        'experiment_attributes' => [],
        'experiment_columns'    => [ 'experiment_source_id', 'experiment_alias', 'library_name' ,'instrument_platform', 'paired_nominal_length'],

        require_run_attributes        => {},
        require_experiment_attributes => {},
        require_study_attributes      => {},
        require_sample_attributes     => {},
        exclude_run_attributes        => {},
        exclude_experiment_attributes => {},
        exclude_study_attributes      => {},
        exclude_sample_attributes     => {},
        require_experiment_columns    => { instrument_platform => ['ILLUMINA'], library_strategy => 'ChIP-Seq'},
        require_run_columns           => { status => ['public', 'private'], },
        require_study_columns         => {},
        require_sample_columns        => {},
        exclude_sample_columns        => {},
        exclude_experiment_columns    => {},
        exclude_run_columns           => {},
        exclude_study_columns         => {},
         

        'dir_label_params_list'       => ["sample_source_id", "experiment_source_id", "run_source_id", "chunk_label"],

        final_output_dir              => $self->o('final_output_dir'),
        final_output_layout           => '#sample_desc_1#/#sample_desc_2#/#sample_desc_3#/#library_strategy#/#center_name#',
        name_file_module              => 'ReseqTrack::Hive::NameFile::BaseNameFile',
        name_file_method              => 'basic',
        name_file_params              => {
                                           new_dir       => '#final_output_dir#/#final_output_layout#',
                                           new_basename  => '#run_source_id#.bwa.GRCh38',
                                           add_datestamp => 1,
                                           suffix         => '.bam',
                                         },
        
       };
}

sub pipeline_create_commands {
    my ($self) = @_;

    return [
        @{$self->SUPER::pipeline_create_commands},
    ];
}

sub pipeline_wide_parameters {
    my ($self) = @_;
    return {
        %{$self->SUPER::pipeline_wide_parameters},

         dir_label_params => $self->o('dir_label_params_list'),
         species_name     => $self->o('species_name'),     
         genome_build     => $self->o('genome_build'),
    };
}

sub resource_classes {
    my ($self) = @_;
    return {
            %{$self->SUPER::resource_classes},  # inherit 'default' from the parent class
            '200Mb' => { 'LSF' => '-C0 -M200 -q '.$self->o('lsf_queue').' -R"select[mem>200] rusage[mem=200]"' },
            '500Mb' => { 'LSF' => '-C0 -M500 -q '.$self->o('lsf_queue').' -R"select[mem>500] rusage[mem=500]"' },
            '1Gb'   => { 'LSF' => '-C0 -M1000 -q '.$self->o('lsf_queue').' -R"select[mem>1000] rusage[mem=1000]"' },
            '2Gb' => { 'LSF' => '-C0 -M2000 -q '.$self->o('lsf_queue').' -R"select[mem>2000] rusage[mem=2000]"' },
            '4Gb' => { 'LSF' => '-C0 -M4000 -q '.$self->o('lsf_queue').' -R"select[mem>4000] rusage[mem=4000]"' },
            '5Gb' => { 'LSF' => '-C0 -M5000 -q '.$self->o('lsf_queue').' -R"select[mem>5000] rusage[mem=5000]"' },
            '6Gb' => { 'LSF' => '-C0 -M6000 -q '.$self->o('lsf_queue').' -R"select[mem>6000] rusage[mem=6000]"' },
            '8Gb' => { 'LSF' => '-C0 -M8000 -q '.$self->o('lsf_queue').' -R"select[mem>8000] rusage[mem=8000]"' },
    };
}



sub pipeline_analyses {
    my ($self) = @_;

    my @analyses;
    push(@analyses, {
            -logic_name    => 'get_seeds',
            -module        => 'ReseqTrack::Hive::Process::SeedFactory',
            -meadow_type => 'LOCAL',
            -parameters    => {
                seeding_module => $self->o('seeding_module'),
                seeding_options => $self->o('seeding_options'),
            },
            -flow_into => {
                2 => [ 'find_source_fastqs' ],
            },
      });
      push(@analyses, {
            -logic_name    => 'find_source_fastqs',
            -module        => 'ReseqTrack::Hive::Process::ImportCollection',
            -meadow_type => 'LOCAL',
            -parameters    => {
                collection_type => $self->o('type_fastq'),
                collection_name => '#run_source_id#',
                output_param => 'fastq',
                reseqtrack_options => {
                  flows_do_count_param => 'fastq',
                  flows_do_count => { 
                                       1 => '1+',
                                       2 => '0',
                                    },
                }
            },
            -flow_into => {
                '1' => [ 'split_fastq' ],
                '2' => [ 'mark_seed_futile' ],
            },
      });
      push(@analyses, {
            -logic_name    => 'split_fastq',
            -module        => 'ReseqTrack::Hive::Process::SplitFastq',
            -parameters    => {
                program_file => $self->o('split_exe'),
                max_reads    => $self->o('chunk_max_reads'),
                regexs       => $self->o('regexs'),
            },
            -rc_name => '200Mb',
            -analysis_capacity  =>  4,
            -hive_capacity  =>  200,
            -flow_into => {
              '2->A' => {'bwa' => {'chunk_label' => '#run_source_id#.#chunk#', 'fastq' => '#fastq#'}},
              'A->1' => ['merge_chunks'],
            }
      });
      push(@analyses, {
           -logic_name => 'bwa',
            -module        => 'ReseqTrack::Hive::Process::BWA',
            -parameters    => {
                program_file => $self->o('bwa_exe'),
                samtools => $self->o('samtools_exe'),
                reference => $self->o('reference'),
                options => $self->o('bwa_options'),
                RGSM => $self->o('RGSM'),
                RGPU => $self->o('RGPU'),
                reseqtrack_options => {
                  delete_param => 'fastq',
                },
            },
            -rc_name => '6Gb', # Note the 'hardened' version of BWA may need 8Gb RAM or more
            -hive_capacity  =>  100,
            -flow_into => {
                1 => ['sort_chunks'],
            },
      });
      push(@analyses, {
            -logic_name => 'sort_chunks',
            -module        => 'ReseqTrack::Hive::Process::RunPicard',
            -parameters => {
                picard_dir => $self->o('picard_dir'),
                bwa_algorithm => $self->o('bwa_algorithm'),
                command => '#expr(#bwa_algorithm#=="sw" ? "add_or_replace_read_groups" : "fix_mate")expr#',
                options => {read_group_fields => {
                  ID => '#run_source_id#',
                  LB => '#library_name#',
                  PL => '#instrument_platform#',
                  PU => $self->o('RGPU'),
                  SM => $self->o('RGSM'),
                  CN => '#center_name#',
                  DS => '#study_source_id#',
                  PI => '#paired_nominal_length#',
                }, },
                create_index => 1,
                jvm_args => '-Xmx2g',
                reseqtrack_options => {
                  delete_param => 'bam',
                },
            },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => [ ':////accu?bam=[]', ':////accu?bai=[]']
            },
      });
      push(@analyses, {
          -logic_name => 'merge_chunks',
          -module        => 'ReseqTrack::Hive::Process::RunPicard',
          -parameters => {
              picard_dir => $self->o('picard_dir'),
              jvm_args => '-Xmx2g',
              command => 'merge',
              create_index => 1,
              reseqtrack_options => {
                delete_param => ['bam', 'bai'],
              }
          },
          -rc_name => '2Gb',
          -hive_capacity  =>  200,
          -flow_into => {
              1 => [ 'reheader' ],
          },
    });
      push(@analyses, {
            -logic_name => 'reheader',
            -module        => 'ReseqTrack::Hive::Process::ReheaderBam',
            -parameters => {
                'samtools'          => $self->o('samtools_exe'),
                'header_lines_file' => $self->o('header_lines_file'),
                'dict_file'         => $self->o('dict_file'),
                'reference'         => $self->o('reference'),
                'SQ_assembly'       => $self->o('ref_assembly'),
                'SQ_species'        => $self->o('ref_species'),
                'SQ_uri'            => $self->o('reference_uri'),
                reseqtrack_options => {
                #  denestify => [ 'fastq', 'bam', 'bai' ],
                  delete_param => ['bam', 'bai'],
                },
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => ['final_index'],
            },
      });
      push(@analyses, {
            -logic_name => 'final_index',
            -module        => 'ReseqTrack::Hive::Process::RunSamtools',
            -parameters => {
                program_file => $self->o('samtools_exe'),
                command      => 'index',
            },
            -flow_into => {1 => ['store_bam']},
            -rc_name   => '200Mb',
            -hive_capacity  =>  200,
      });
      push(@analyses, {
            -logic_name    => 'store_bam',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('bam_type'),
              file => '#bam#',
              name_file_module    => $self->o('name_file_module'),
              name_file_method    => $self->o('name_file_method'),
              name_file_params    => $self->o('name_file_params'),
              final_output_dir    => $self->o('final_output_dir'),
              final_output_layout => $self->o('final_output_layout'),
              collection_name     => $self->o('collection_name'),
              collect             => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'store_bai' => {'final_bam' => '#file#'}}},
      });
    push(@analyses, {
            -logic_name    => 'store_bai',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('bai_type'),
              file => '#bai#',
              name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
              name_file_method => 'basic',
              name_file_params => {new_full_path => '#final_bam#.bai'},
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'validate' => {'bai' => '#file#', bam => '#final_bam#' }}},
      });
    push(@analyses, {
            -logic_name => 'validate',
            -module        => 'ReseqTrack::Hive::Process::RunValidateBam',
            -parameters => {
                'program_file' => $self->o('validate_bam_exe'),
                 'bam'         => '#bam#',
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => ['store_bas']},
      });
      push(@analyses, {
            -logic_name    => 'store_bas',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -meadow_type => 'LOCAL',
            -parameters    => {
              type => $self->o('bas_type'),
              file => '#bas#',
              name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
              name_file_method => 'basic',
              name_file_params => {new_full_path => '#bam#.bas'},
            },
            -flow_into => {1 => {'mark_seed_complete' => {'bas' => '#file#'}}},
      });
    push(@analyses, {
            -logic_name    => 'mark_seed_complete',
            -module        => 'ReseqTrack::Hive::Process::UpdateSeed',
            -parameters    => {
              is_complete  => 1,
            },
            -meadow_type => 'LOCAL',
      });
    push(@analyses, {
            -logic_name    => 'mark_seed_futile',
            -module        => 'ReseqTrack::Hive::Process::UpdateSeed',
            -parameters    => {
              is_futile  => 1,
            },
            -meadow_type => 'LOCAL',
      });
      return \@analyses;
}

1;
      
      
