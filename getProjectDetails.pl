#!/usr/bin/env perl
use strict;
use warnings;
use XML::Twig;
use Data::Dumper;
use HTTP::Tiny;
use Time::HiRes;

my $project = shift;
die "Usage: $0 project_id\n" if not $project;

my $http = HTTP::Tiny->new();
my $requestCount = 0;
my $lastRequestTime = 0; 
my @fields = qw /
    study_accession
    sample_accession
    secondary_sample_accession
    experiment_accession
    run_accession
    tax_id
    scientific_name
    instrument_model
    library_layout
    fastq_ftp
    fastq_galaxy
    submitted_ftp
    submitted_galaxy
    sra_ftp
    sra_galaxy
    cram_index_ftp
    cram_index_galaxy
/;

getProjectXml($project);

my %samp_to_run = getSamplesAndFiles($project);

outputRunDetails(\%samp_to_run);

##################################################
sub outputRunDetails{
    my $samp = shift;
    my $tab_out = "$project.run.details.txt";
    warn "Outputting read/run details to $tab_out...\n";
    open (my $DETAILS, ">", "$tab_out") or die "Could not open $tab_out for writing: $!\n";
    print $DETAILS join("\t", (qw / 
            run_accession
            sample_accession
            exp_ref_name
            exp_ref
            title
        /)
    ) . "\n" ;
    foreach my $k (sort keys %$samp){
        my $r_url = "http://www.ebi.ac.uk/ena/data/view/" 
                    . $samp->{$k} 
                    . "&display=xml&download=xml" ;
        my $xml = restQuery($r_url);
        if (my @details = parseRunXml($xml)){
            print $DETAILS join("\t", $samp->{$k}, $k, @details) . "\n";
        }
    }
    close $DETAILS
}

##################################################
sub parseRunXml{
    my $x = shift;
    my $twig= new XML::Twig;
    $twig->parse($x);
    my $root= $twig->root;           # get the root of the twig (stats)
    my @children = $root->children;
    my $n = 0;
    foreach my $s (@children){
        $n++;
        my $samp_id = '';
        my $ids = $s->first_child("IDENTIFIERS");
        my $title = $s->first_child("TITLE");
        my $exp = $s->first_child("EXPERIMENT_REF");
        if ($title->text){
            return ($ids->text, $title->text, $exp->text);
        }else{
            warn "No condition found for sample $n\n";
        }
    }
}
##################################################
sub getProjectXml{
    my $p_url = "http://www.ebi.ac.uk/ena/data/view/$project&display=xml";
    my $xml_out = "$project.xml";
    warn "Outputting project XML to $xml_out...\n";
    open (my $PROJ, ">", "$xml_out") or die "Could not open $xml_out for writing: $!\n";
    print $PROJ restQuery($p_url);
    close $PROJ;
}

##################################################
sub getSamplesAndFiles{
    my $p = shift;
    my $files_out = "$project.txt";
    open (my $FILES, ">", $files_out) or die "Could not open $files_out for writing: $!\n";
    my $r_url = "http://www.ebi.ac.uk/ena/data/warehouse/filereport"
     . "?accession=$project"
     . "&result=read_run&fields="
     . join(",", @fields) 
     . "&download=txt";
    my $table = restQuery($r_url);
    warn "Outputting file table to $files_out...\n";
    print $FILES $table;
    close $FILES;
    my ($h, @lines) = split("\n", $table);
    my %tabcol = ();
    my @head = split("\t", $h); 
    my %samp_to_run = (); 
    foreach my $c (qw / sample_accession run_accession / ){ 
        no warnings 'uninitialized';
        $tabcol{$c}++ until $head[$tabcol{$c}] eq $c or $tabcol{$c} > $#head;
        die "Could not find '$c' column in $table header!\n" if $tabcol{$c} > $#head;
    }
    foreach my $line (@lines){
        my @split = split("\t", $line);
        $samp_to_run{$split[$tabcol{sample_accession}]} = 
          $split[$tabcol{run_accession}];
    }
    return %samp_to_run;
}


    
##################################################
sub restQuery{
    my $url = shift;
    $requestCount++;    
    if ($requestCount == 15) { # check every 15
        my $current_time = Time::HiRes::time();
        my $diff = $current_time - $lastRequestTime;
        # if less than a second then sleep for the remainder of the second
        if($diff < 1) {
            Time::HiRes::sleep(1-$diff);
        }
        # reset
        $requestCount = 0;
    }
    $lastRequestTime = Time::HiRes::time();
    my $response = $http->get($url,# {
         # headers => { 'Content-type' => 'application/json' }
        #}
    );
    my $status = $response->{status};
    if (not $response->{success}){
        if($status == 429 && exists $response->{headers}->{'retry-after'}) {
            my $retry = $response->{headers}->{'retry-after'};
            Time::HiRes::sleep($retry);
            return restQuery($url); 
        }
        my $reason = $response->{reason};
        warn "Ensembl REST query ('$url') failed: Status code: ${status}. Reason: ${reason}\n" ;
        return;
    }

    if(length $response->{content}) {
        return $response->{content};
    }
    warn "No content for Ensembl REST query ('$url')!\n";
    return;
}
