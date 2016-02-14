package ReseqTrack::Hive::PipeConfig::RunMerge_conf;

use strict;
use warnings;

use base ('ReseqTrack::Hive::PipeConfig::ReseqTrackGeneric_conf');

sub default_options {
    my ($self) = @_;

    return {
        %{ $self->SUPER::default_options() },

        'pipeline_name' => 'run_merge',
        seeding_module  => 'ReseqTrack::Hive::PipeSeed::RunMergeSeed',
        seeding_options => {
          output_study_columns           => $self->o('study_columns'),
          output_sample_columns          => $self->o('sample_columns'),
          output_columns                 => $self->o('experiment_columns'),
          output_run_columns             => $self->o('run_columns'),
          output_study_attributes        => $self->o('study_attributes'),
          output_sample_attributes       => $self->o('sample_attributes'),
          output_experiment_attributes   => $self->o('experiment_attributes'),
          output_run_attributes          => $self->o('run_attributes'),
          require_study_columns          => $self->o('require_study_columns'),
          require_sample_columns         => $self->o('require_sample_columns'),
          require_columns                => $self->o('require_experiment_columns'),
          require_run_columns            => $self->o('require_run_columns'),
          exclude_study_columns          => $self->o('exclude_study_columns'),
          exclude_sample_columns         => $self->o('exclude_sample_columns'),
          exclude_experiment_columns     => $self->o('exclude_experiment_columns'),
          exclude_run_columns            => $self->o('exclude_run_columns'),
          require_study_attributes       => $self->o('require_study_attributes'),
          require_sample_attributes      => $self->o('require_sample_attributes'),
          require_experiment_attributes  => $self->o('require_experiment_attributes'),
          require_run_attributes         => $self->o('require_run_attributes'),
          exclude_study_attributes       => $self->o('exclude_study_attributes'),
          exclude_sample_attributes      => $self->o('exclude_sample_attributes'),
          exclude_experiment_attributes  => $self->o('exclude_experiment_attributes'),
          exclude_run_attributes         => $self->o('exclude_run_attributes'),
          experiment_collection_type     => $self->o('experiment_collection_type'),
          experiment_merge_type          => $self->o('experiment_merge_type'),
          run_collection_type            => $self->o('run_collection_type'),
          metadata_file                  => $self->o('metadata_file'),
          path_names_array               => $self->o('path_names_array'),
        },
         
        metadata_file        => $self->o('metadata_file'),
        path_names_array     => [ 'sample_desc_1', 'sample_desc_2', 'sample_desc_3', 'library_strategy', 'center_name' ],
        species_name         => 'homo_sapiens',
        genome_build         => 'GRCh38',
          
        'root_output_dir'    => $self->o('root_output_dir'),
        'final_output_dir'   => $self->o('final_output_dir'),

        'reference'          => $self->o('reference'),
        'collection_name'    => '#experiment_source_id#', 
        'build_collection'   => 1,
   
        'picard_dir'         => '/nfs/1000g-work/G1K/work/bin/picard',
        'samtools_exe'       => '/nfs/1000g-work/G1K/work/bin/samtools/samtools',
        'ppqt'               => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/phantomPeakQualTools/run_spp_nodups.R',
        'ppqt_rscript_path'  => '/nfs/1000g-work/G1K/work/bin/R/bin/Rscript',

        'picard_remove_duplicates' => 0,
        'mapq_threshold'           => 5,
        'duplicate_flag_value'     => '1024', ## sam flag for PCR or optical duplicate
        'filter_duplicates'        => 1,
        'unmapped_read_flag'       => 4,
        'sam_qual_filter_options'  => { mapq_threshold => $self->o('mapq_threshold'),
                                        flag_value     => $self->o('unmapped_read_flag'),
                                        remove_flag    => 1,
                                      },
        'sam_dedup_filter_options' => { flag_value     => $self->o('duplicate_flag_value'),
                                        remove_flag    => $self->o('filter_duplicates'),
                                      },
        'sample_attributes'     => [],
        'sample_columns'        => ['sample_id', 'sample_source_id', 'sample_alias'],
        'run_attributes'        => [],
        'run_columns'           => ['run_id', 'run_source_id', 'center_name', 'run_alias'],
        'study_attributes'      => [],
        'study_columns'         => ['study_source_id'],
        'experiment_attributes' => [ 'EXPERIMENT_TYPE' ],
        'experiment_columns'    => [ 'experiment_source_id', 'experiment_alias', 'instrument_platform', 'paired_nominal_length'],

        require_run_attributes        => {},
        require_experiment_attributes => {},
        require_study_attributes      => {},
        require_sample_attributes     => {},
        exclude_run_attributes        => {},
        exclude_experiment_attributes => {},
        exclude_study_attributes      => {},
        exclude_sample_attributes     => {},
        require_experiment_columns    => { instrument_platform => ['ILLUMINA'], library_strategy => 'ChIP-Seq', status => ['public', 'private'] },
        require_run_columns           => { status => ['public', 'private'], },
        require_study_columns         => {},
        require_sample_columns        => {},
        exclude_sample_columns        => {},
        exclude_experiment_columns    => {},
        exclude_run_columns           => {},
        exclude_study_columns         => {},
        
        experiment_collection_type => 'CHIP_DEDUP_BAM',
        unfilt_bam_type            => 'CHIP_UNFILTER_BAM',
        filt_bam_type              => 'CHIP_QUAL_FILTER_BAM',
        dedup_bam_type             => 'CHIP_DEDUP_BAM',
        unfilt_flagstat_type       => 'CHIP_UNFILTER_FLAGSTAT',
        filt_flagstat_type         => 'CHIP_QUAL_FILTER_FLAGSTAT',
        dedup_flagstat_type        => 'CHIP_DEDUP_FLAGSTAT',
        ppqt_metrics_type          => 'CHIP_PPQT_METRICS',
        ppqt_pdf_type              => 'CHIP_PPQT_PDF',
        experiment_merge_type      => 'CHIP_MERGE_RUN_BAM',
        run_collection_type        => 'CHIP_RUN_BAM',
        rg_tag_name                => 'ID',


        'wiggler_exe'             => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/align2rawsignal/bin/align2rawsignal',
        'bedGraph_to_bigWig_path' => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/bin/bedGraphToBigWig',
        'output_format'           => 'bw',
        'chrom_sizes_file'        => $self->o('chrom_sizes_file'),
        'chrom_fasta_file'        => '/nfs/1000g-work/G1K/work/davidr/ref_genomes/homo_sapiens/grch38_no_alt_analysis/fasta/',
        'mappability_tracks'      => '/nfs/1000g-work/G1K/work/davidr/ref_genomes/homo_sapiens/grch38_no_alt_analysis/umap/globalmap_k36tok92',
        'mcr_root'                => '/nfs/1000g-work/G1K/work/davidr/pipeline-deps/MCR/v714',
        'wiggler_options'         =>  {},
        'build_collection'        => 1,

        'dir_label_params_list' => ["sample_source_id", "experiment_source_id", "run_source_id" ],
        
        final_output_layout     => '#sample_desc_1#/#sample_desc_2#/#sample_desc_3#/#library_strategy#/#center_name#',

        name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
        name_file_method => 'basic',
        name_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.bwa.GRCh38',              
            add_datestamp => 1,
            suffix        => '.bam',
          },
        unfilt_name_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.unfiltered.bwa.GRCh38',  
            add_datestamp => 1,
            suffix        => '.bam',
          },
        filt_name_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.quality_filtered.bwa.GRCh38', 
            add_datestamp => 1,
            suffix        => '.bam',
          },
        dedup_name_file_params => {
            new_dir       => '#final_output_dir#/#final_output_layout#',
            new_basename  => '#sample_alias#.#experiment_source_id#.#EXPERIMENT_TYPE#.dedup.bwa.GRCh38',  
            add_datestamp => 1,
            suffix        => '.bam',
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
    };
}

sub resource_classes {
    my ($self) = @_;
    return {
            %{$self->SUPER::resource_classes},  # inherit 'default' from the parent class
            '200Mb' => { 'LSF' => '-C0 -M200 -q '.$self->o('lsf_queue').' -R"select[mem>200] rusage[mem=200]"' },
            '500Mb' => { 'LSF' => '-C0 -M500 -q '.$self->o('lsf_queue').' -R"select[mem>500] rusage[mem=500]"' },
            '1Gb' => { 'LSF' => '-C0 -M1000 -q '.$self->o('lsf_queue').' -R"select[mem>1000] rusage[mem=1000]"' },
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
                2 => [ 'runs_factory' ],
            },
      });
     push(@analyses, {
            -logic_name    => 'runs_factory',
            -module        => 'ReseqTrack::Hive::Process::RunMetaInfoFactory',
            -meadow_type => 'LOCAL',
            -parameters    => {
                factory_type => 'run',
                require_experiment_columns    => $self->o('require_experiment_columns'),
                require_study_columns         => $self->o('require_study_columns'),
                require_run_columns           => $self->o('require_run_columns'),
                require_experiment_attributes => $self->o('require_experiment_attributes'),
                require_study_attributes      => $self->o('require_study_attributes'),
                require_run_attributes        => $self->o('require_run_attributes'),
                exclude_experiment_attributes => $self->o('exclude_experiment_attributes'),
                exclude_study_attributes      => $self->o('exclude_study_attributes'),
                exclude_run_attributes        => $self->o('exclude_run_attributes'),
                output_run_columns            => $self->o('run_columns'),
                output_study_columns          => $self->o('study_columns'),
                output_experiment_columns     => $self->o('experiment_columns'),
                output_run_attributes         => $self->o('run_attributes'),
                output_study_attributes       => $self->o('study_attributes'),
                output_experiment_attributes  => $self->o('experiment_attributes'),
            },
            -flow_into => {
                '2->A' => [ 'find_source_bams' ],
                'A->1' => [ 'decide_merge' ],
            },

      });
      push(@analyses, {
            -logic_name    => 'find_source_bams',
            -module        => 'ReseqTrack::Hive::Process::ImportCollection',
            -meadow_type => 'LOCAL',
            -parameters    => {
                collection_type => $self->o('run_collection_type'),
                collection_name => '#run_source_id#',
                output_param => 'bam',
            },
            -flow_into => {
                1 => [ ':////accu?bam=[]' ]
            },
      });
      push(@analyses, {
          -logic_name => 'decide_merge',
          -module        => 'ReseqTrack::Hive::Process::BaseProcess',
          -meadow_type=> 'LOCAL',
          -parameters => {
              reseqtrack_options => {
                denestify => [ 'bam' ],
                flows_do_count_param => 'bam',
                flows_non_factory => { 
                                        1 => 1,
                                        2 => 1,
                                     },
                flows_do_count => { 1 => '1+',
                                    2 => '0', 
                                  },
              }
          },
          -flow_into => {
              1 => { 'merge_bams' => {'input_bam' => '#bam#' }},
              2 => [ 'mark_seed_futile' ],
          },
      });
      push(@analyses, {
          -logic_name => 'merge_bams',
          -module        => 'ReseqTrack::Hive::Process::RunPicard',
          -parameters => {
              picard_dir => $self->o('picard_dir'),
              bam => '#input_bam#',
              jvm_args => '-Xmx2g',
              command => 'merge',
              create_index => 1,
              
          },
          -rc_name => '2Gb',
          -hive_capacity  =>  200,
          -flow_into => { 1 =>  ['create_merge_collection'] },
      });
      push(@analyses, {
            -logic_name    => 'mark_seed_futile',
            -module        => 'ReseqTrack::Hive::Process::UpdateSeed',
            -parameters    => {
              is_futile => 1,  ## fix issues before trying another time
            },
            -meadow_type => 'LOCAL',
      });
      push(@analyses, {
          -logic_name => 'create_merge_collection',
          -module        => 'ReseqTrack::Hive::Process::BlueprintCreateMergeCollection',
          -parameters => {
               samtools => $self->o('samtools_exe'), 
               collection_type => $self->o('experiment_merge_type'),
               collection_name => $self->o('collection_name'),
               rg_tag_name  => $self->o('rg_tag_name'),  
          }, 
          -meadow_type => 'LOCAL',
          -flow_into => { 1 =>  ['mark_duplicates'] },
      }); 
      push(@analyses, {
            -logic_name => 'mark_duplicates',
            -module        => 'ReseqTrack::Hive::Process::RunPicard',
            -parameters => {
                picard_dir => $self->o('picard_dir'),
                jvm_args => '-Xmx4g',
                command => 'mark_duplicates',
                options => {'shorten_input_names' => 1},
                create_index => 1,
                add_attributes => 1,
                remove_duplicates =>$self->o('picard_remove_duplicates'),
                reseqtrack_options => {
                  delete_param => ['bam', 'bai'],
               }
            },
            -rc_name => '5Gb',
            -hive_capacity  =>  100,
            -flow_into => {
                1 => 
                   { 'store_unfilt_bam' => 
                    { 'markdup_attribute_metrics' => '#attribute_metrics#',
                      'bam' => '#bam#' }},
            },
      });
      push(@analyses, {
            -logic_name    => 'store_unfilt_bam',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('unfilt_bam_type'),
              file => '#bam#',
              name_file_module => $self->o('name_file_module'),
              name_file_method => $self->o('name_file_method'),
              name_file_params => $self->o('unfilt_name_file_params'),
              final_output_dir => $self->o('final_output_dir'),
              final_output_layout => $self->o('final_output_layout'),
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'unfilt_flagstat' => {'bam' => '#file#'}}},
      });
      push(@analyses, {
            -logic_name => 'unfilt_flagstat',
            -module        => 'ReseqTrack::Hive::Process::RunSamtools',
            -parameters => {
                program_file => $self->o('samtools_exe'),
                command => 'flagstat',
                add_attributes => 1,
                reference => $self->o('reference'),
            },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'unfilt_attributes' =>
                        { 'unfilt_attribute_metrics' => '#attribute_metrics#',
                          'unfilt_metrics' => '#metrics#'}},
            },
      });
      push(@analyses, {
            -logic_name => 'unfilt_attributes',
            -module        => 'ReseqTrack::Hive::Process::UpdateAttribute',
            -parameters => {
                attribute_metrics => [ '#unfilt_attribute_metrics#', '#markdup_attribute_metrics#' ] ,
                collection_type => $self->o('unfilt_bam_type'),
                collection_name => $self->o('collection_name'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => [ 'store_unfilt_flagstat'],
            },
      });
      push(@analyses, {
            -logic_name    => 'store_unfilt_flagstat',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('unfilt_flagstat_type'),
              file => '#unfilt_metrics#',
              name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
              name_file_method => 'basic',
              name_file_params => {new_full_path => '#bam#.flagstat'},
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'quality_filter' => {'bam' => '#bam#'}}},
      });
      push(@analyses, {
            -logic_name => 'quality_filter',
            -module        => 'ReseqTrack::Hive::Process::RunSamtools',
            -parameters => {
                program_file => $self->o('samtools_exe'),
                command => 'filter',
                samtools_options => $self->o('sam_qual_filter_options'),
            },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'store_filt_bam' => { 'bam' => '#bam#' }},
            },
      });
      push(@analyses, {
            -logic_name    => 'store_filt_bam',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('filt_bam_type'),
              file => '#bam#',
              name_file_module => $self->o('name_file_module'),
              name_file_method => $self->o('name_file_method'),
              name_file_params => $self->o('filt_name_file_params'),
              final_output_dir => $self->o('final_output_dir'),
              final_output_layout => $self->o('final_output_layout'),
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'qual_filter_flagstat' => {'bam' => '#file#'}}},
      });
      push(@analyses, {
            -logic_name => 'qual_filter_flagstat',
            -module        => 'ReseqTrack::Hive::Process::RunSamtools',
            -parameters => {
                program_file => $self->o('samtools_exe'),
                command => 'flagstat',
                add_attributes => 1,
                reference => $self->o('reference'),
            },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'store_qual_filter_flagstat' =>
                       { 'qual_filter_attribute_metrics' => '#attribute_metrics#',
                         'qual_filter_metrics' => '#metrics#'  }},
            },
      });
      push(@analyses, {
            -logic_name    => 'store_qual_filter_flagstat',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('filt_flagstat_type'),
              file => '#qual_filter_metrics#',
              name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
              name_file_method => 'basic',
              name_file_params => {new_full_path => '#bam#.flagstat'},
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'qual_filter_attributes' => {'bam' => '#bam#'}}},
      });
      push(@analyses, {
            -logic_name => 'qual_filter_attributes',
            -module        => 'ReseqTrack::Hive::Process::UpdateAttribute',
            -parameters => {
                attribute_metrics => [ '#qual_filter_attribute_metrics#' ] ,
                collection_type => $self->o('filt_bam_type'),
                collection_name => $self->o('collection_name'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => [ 'dedup_bam'],
            },
      });      
      push(@analyses, {
            -logic_name => 'dedup_bam',
            -module        => 'ReseqTrack::Hive::Process::RunSamtools',
            -parameters => {
                program_file => $self->o('samtools_exe'),
                command => 'filter',
                samtools_options => $self->o('sam_dedup_filter_options'),
                },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'store_dedup_bam' => { 'bam' => '#bam#' }},
            },
      });
      push(@analyses, {
            -logic_name    => 'store_dedup_bam',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('dedup_bam_type'),
              file => '#bam#',
              name_file_module => $self->o('name_file_module'),
              name_file_method => $self->o('name_file_method'),
              name_file_params => $self->o('dedup_name_file_params'),
              final_output_dir => $self->o('final_output_dir'),
              final_output_layout => $self->o('final_output_layout'),
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },  
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'dedup_index' => {'bam' => '#file#'}}},
      });
      push(@analyses, {
            -logic_name => 'dedup_index',
            -module        => 'ReseqTrack::Hive::Process::RunSamtools',
            -parameters => {
                program_file => $self->o('samtools_exe'),
                command => 'index',
                reference => $self->o('reference'),
                },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => [ 'dedup_flagstat' ],
            },
      });
      push(@analyses, {
            -logic_name => 'dedup_flagstat',
            -module        => 'ReseqTrack::Hive::Process::RunSamtools',
            -parameters => {
                program_file => $self->o('samtools_exe'),
                command => 'flagstat',
                add_attributes => 1,
                reference => $self->o('reference'),
            },
            -rc_name => '2Gb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'store_dedup_flagstat' =>
                       { 'dedup_attribute_metrics' => '#attribute_metrics#',
                         'dedup_metrics' => '#metrics#'  }},
            },
      });
      push(@analyses, {
            -logic_name    => 'store_dedup_flagstat',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('dedup_flagstat_type'),
              file => '#dedup_metrics#',
              name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
              name_file_method => 'basic',
              name_file_params => {new_full_path => '#bam#.flagstat'},
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => {'ppqt' => {'dedup_bam' => '#bam#'}}},
       });
       push(@analyses, {
          -logic_name => 'ppqt',
          -module        => 'ReseqTrack::Hive::Process::PPQT',
          -parameters => {
             program_file => $self->o('ppqt'),
             rscript_path => $self->o('ppqt_rscript_path'),
             samtools => $self->o('samtools_exe'), 
             keep_metrics_file => 1,
             keep_plot => 1,
             keep_rdata => 1,
             add_attributes => 1,
          },
          -rc_name => '2Gb',
          -hive_capacity  =>  200,
          -flow_into => {
                1 => { 'dedup_attributes' =>
                       { 'ppqt_attribute_metrics' => '#attribute_metrics#',
                         'ppqt_metrics' => '#ppqt_metrics#',
                         'ppqt_pdf' => '#ppqt_pdf#'
                       }},
            },
    });
    push(@analyses, {
            -logic_name => 'dedup_attributes',
            -module        => 'ReseqTrack::Hive::Process::UpdateAttribute',
            -parameters => {
                attribute_metrics => [ '#ppqt_attribute_metrics#', '#dedup_attribute_metrics#' ] ,
                collection_type => $self->o('dedup_bam_type'),
                collection_name => $self->o('collection_name'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => { 'store_ppqt_metrics'=> { 'ppqt_metrics' => '#ppqt_metrics#' }},
            },
      }); 
      push(@analyses, {
            -logic_name    => 'store_ppqt_metrics',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('ppqt_metrics_type'),
              file => '#ppqt_metrics#',
              name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
              name_file_method => 'basic',
              name_file_params => {new_full_path => '#bam#.ppqt.metrics'},
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {
                1 => {'store_ppqt_pdf' => {'ppqt_pdf' => '#ppqt_pdf#'}},
            },
      });
      push(@analyses, {
            -logic_name    => 'store_ppqt_pdf',
            -module        => 'ReseqTrack::Hive::Process::LoadFile',
            -parameters    => {
              type => $self->o('ppqt_pdf_type'),
              file => '#ppqt_pdf#',
              name_file_module => 'ReseqTrack::Hive::NameFile::BaseNameFile',
              name_file_method => 'basic',
              name_file_params => {new_full_path => '#bam#.ppqt.pdf'},
              collection_name => $self->o('collection_name'),
              collect => $self->o('build_collection'),
            },
            -rc_name => '200Mb',
            -hive_capacity  =>  200,
            -flow_into => {1 => ['mark_seed_complete'] },
      });
      push(@analyses, {
            -logic_name    => 'mark_seed_complete',
            -module        => 'ReseqTrack::Hive::Process::UpdateSeed',
            -parameters    => {
              is_complete  => 1,
            },
            -meadow_type => 'LOCAL',
      }); 
      return \@analyses;
}

1;

