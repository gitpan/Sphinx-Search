#! /usr/bin/perl

# Copyright 2007 Jon Schutz, all rights reserved.
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License.

# Main functional test for Sphinx::Search
# Loads data into mysql, runs indexer, starts searchd, validates results.

use strict;
use warnings;

use DBI;
use Test::More;
use File::SearchPath qw/searchpath/;
use Path::Class;
use Sphinx::Search;
use Socket;

my $searchd = $ENV{SPHINX_SEARCHD} || searchpath('searchd');
my $indexer = $ENV{SPHINX_INDEXER} || searchpath('indexer');
unless ($searchd && -e $searchd) {
    plan skip_all => "Can't find searchd; set SPHINX_SEARCHD to location of searchd binary in order to run these tests";
};
$indexer = Path::Class::file($searchd)->dir->file('indexer')->stringify unless $indexer;
unless ($indexer && -e $indexer) {
    plan skip_all => "Can't find indexer; set SPHINX_INDEXER to location of indexer binary in order to run these tests";
};

my $dbtable = 'sphinx_test_jjs_092348792';
my $dsn = $ENV{SPHINX_DSN} || "dbi:mysql:database=test";
my $dbuser = $ENV{SPHINX_DBUSER} || "root";
my $dbpass = $ENV{SPHINX_DBPASS} || "";
my $dbname = ( $dsn =~ m!database=([^;]+)! ) ? $1 : "test";
my $dbhost = ( $dsn =~ m!host=([^;]+)! ) ? $1 : "localhost";
my $dbport = ( $dsn =~ m!port=([^;]+)! ) ? $1 : "";
my $sph_port = $ENV{SPHINX_PORT} || int(rand(20000));

my $dbi = DBI->connect($dsn, $dbuser, $dbpass, { RaiseError => 1 });
unless ($dbi) {
    plan skip_all => "Failed to connect to database; set SPHINX_DSN, SPHINX_DBUSER, SPHINX_DBPASS appropriately to run these tests";
}
unless (create_db($dbi)) {
    plan skip_all => "Failed to create database table; set SPHINX_DSN, SPHINX_DBUSER, SPHINX_DBPASS appropriately to run these tests";
}

my $testdir = Path::Class::dir("data")->absolute;
eval { $testdir->mkpath };
if ($@) {
    plan skip_all => "Failed to create 'data' directory; skipping tests. Fix permissions to run test";
}

my $pidfile = $testdir->file('searchd.pid');
my $configfile = $testdir->file('sphinx.conf');
unless (write_config($configfile)) {
    plan skip_all => "Failed to write config file; skipping tests.  Fix permissions to run test";
}

our @pids;
unless (run_indexer($configfile)) {
    plan skip_all => "Failed to run indexer; skipping tests.";
}

unless (run_searchd($configfile)) {
    plan skip_all => "Failed to run searchd; skipping tests.";
}

# Everything is in place; run the tests
plan tests => 31;

my $sphinx = Sphinx::Search->new({ port => $sph_port });
ok($sphinx, "Constructor");

# Basic test on 'a'
my $results = $sphinx->Query("a");
ok($results, "Results for 'a'");
print $sphinx->GetLastError unless $results;
ok($results->{total_found} == 4, "total_found for 'a'");
ok($results->{total} == 4, "total for 'a'");
ok(@{$results->{matches}} == 4, "matches for 'a'");
is_deeply($results->{'words'}, 
	  {
	      'a' => {
		  'hits' => 4,
		  'docs' => 4
		  }
	  },
	  "words for 'a'");
is_deeply($results->{'fields'}, [ qw/field1 field2/ ], "fields for 'a'");
is_deeply($results->{'attrs'}, { attr1 => 1 }, "attributes for 'a'");
my $weights = 1;
$weights *= $_->{weight} for @{$results->{matches}};
ok($weights == 1, "weights for 'a'");

# Rank order test on 'bb'
$sphinx->SetMatchMode(SPH_MATCH_ANY)
    ->SetSortMode(SPH_SORT_RELEVANCE);
$results = $sphinx->Query("bb");
ok($results, "Results for 'bb'");
print $sphinx->GetLastError unless $results;
ok(@{$results->{matches}} == 5, "matches for 'bb'");
my $order_ok = 1;
for (1 .. @{$results->{matches}} - 1) {
    $order_ok = 0, last unless $results->{matches}->[$_ - 1]->{weight} >= $results->{matches}->[$_]->{weight};
}
ok($order_ok, 'SPH_SORT_RELEVANCE');

# Phrase on "ccc dddd"
$sphinx->SetMatchMode(SPH_MATCH_PHRASE)
    ->SetSortMode(SPH_SORT_ATTR_ASC, "attr1");
$results = $sphinx->Query("ccc dddd");
ok($results, "Results for 'ccc dddd'");
print $sphinx->GetLastError unless $results;
ok(@{$results->{matches}} == 3, "matches for 'ccc dddd'");
$order_ok = 1;
for (1 .. @{$results->{matches}} - 1) {
    $order_ok = 0, last unless $results->{matches}->[$_ - 1]->{attr1} <= $results->{matches}->[$_]->{attr1};
}
ok($order_ok, 'SPH_SORT_ATTR_ASC');

# Boolean on "bb ccc"
$sphinx->SetMatchMode(SPH_MATCH_BOOLEAN)
    ->SetSortMode(SPH_SORT_ATTR_DESC, "attr1");
$results = $sphinx->Query("bb ccc");
ok($results, "Results for 'bb ccc'");
print $sphinx->GetLastError unless $results;
ok(@{$results->{matches}} == 4, "matches for 'bb ccc'");
$order_ok = 1;
for (1 .. @{$results->{matches}} - 1) {
    $order_ok = 0, last unless $results->{matches}->[$_ - 1]->{attr1} >= $results->{matches}->[$_]->{attr1};
}
ok($order_ok, 'SPH_SORT_ATTR_DESC');

# Any on "bb ccc"
$sphinx->SetMatchMode(SPH_MATCH_ANY)
    ->SetSortMode(SPH_SORT_EXTENDED, '@relevance DESC, attr1 ASC');
$results = $sphinx->Query("bb ccc");
ok($results, "Results for 'bb ccc' ANY");
print $sphinx->GetLastError unless $results;
ok(@{$results->{matches}} == 5, "matches for 'bb ccc' ANY");
$order_ok = 1;
for (1 .. @{$results->{matches}} - 1) {
    $order_ok = 0, last unless
	($results->{matches}->[$_]->{weight} <=> $results->{matches}->[$_-1]->{weight} || 
	 $results->{matches}->[$_ - 1]->{attr1} <=> $results->{matches}->[$_]->{attr1}) <= 0;
}
ok($order_ok, 'SPH_SORT_EXTENDED');

$sphinx->SetMatchMode(SPH_MATCH_ANY)
    ->SetSortMode(SPH_SORT_RELEVANCE)
    ->SetLimits(0,2);
$results = $sphinx->Query("bb");
ok($results, "Results for 'bb' with limit");
print $sphinx->GetLastError unless $results;
ok(@{$results->{matches}} == 2, "matches for 'bb'");

# Extended on "bb ccc"
$sphinx->SetMatchMode(SPH_MATCH_EXTENDED)
    ->SetLimits(0,20);
$results = $sphinx->Query('@field1 bb @field2 ccc');
ok($results, "Results for 'bb ccc' EXTENDED");
print $sphinx->GetLastError unless $results;
ok(@{$results->{matches}} == 2, "matches for 'bb ccc' EXTENDED");
ok($results->{matches}->[0]->{doc} =~ m/^(?:4|5)$/ &&
   $results->{matches}->[1]->{doc} =~ m/^(?:4|5)$/, "matched docs for 'bb ccc' EXTENDED");

$sphinx->SetMatchMode(SPH_MATCH_ANY)
    ->SetSortMode(SPH_SORT_RELEVANCE)
    ->SetWeights([10, 2]);
$results = $sphinx->Query("bb ccc");
ok($results, "Results for 'bb ccc'");
print $sphinx->GetLastError unless $results;
$order_ok = 1;
for (1 .. @{$results->{matches}} - 1) {
    $order_ok = 0, last unless $results->{matches}->[$_ - 1]->{weight} >= $results->{matches}->[$_]->{weight} && $results->{matches}->[$_]->{weight} > 1;
}
ok($order_ok, 'Weighted relevance');

# Excerpts
$results = $sphinx->BuildExcerpts([ "bb bb ccc dddd", "bb ccc dddd" ],
		       "test_jjs_index",
		       "ccc dddd");
is_deeply($results, [ 'bb bb <b>ccc</b> <b>dddd</b>', 'bb <b>ccc</b> <b>dddd</b>' ],
	  "Excerpts");

# Update
$sphinx->UpdateAttributes("test_jjs_index", [ qw/attr1/ ], 
			  { 
			      1 => [ 10 ],
			      2 => [ 10 ],
			      3 => [ 20 ],
			      4 => [ 20 ],
			  });
# Verify update with grouped search
$sphinx->SetMatchMode(SPH_MATCH_ANY)
    ->SetSortMode(SPH_SORT_RELEVANCE)
    ->SetGroupBy("attr1", SPH_GROUPBY_ATTR);
$results = $sphinx->Query("bb");
ok($results, "Results for 'bb'");
print $sphinx->GetLastError unless $results;
ok($results->{total} == 3, "Update attributes, grouping");

#use Data::Dumper;
#print Dumper($results);


sub create_db {
    my ($dbi) = @_;

    eval {
	$dbi->do(qq{DROP TABLE IF EXISTS \`$dbtable\`});
	$dbi->do(qq{CREATE TABLE \`$dbtable\` (
					     \`id\` INT NOT NULL auto_increment,
					     \`field1\` TEXT,
					     \`field2\` TEXT,
				             \`attr1\` INT NOT NULL,
					     PRIMARY KEY (\`id\`))});
    $dbi->do(qq{INSERT INTO \`$dbtable\` (\`field1\`,\`field2\`,\`attr1\`) VALUES
		   ('a', 'bb', 2),
		   ('a', 'bb ccc', 4),
		   ('a', 'bb ccc dddd', 1),
		   ('a bb', 'bb ccc dddd', 5),
		   ('bb', 'bb bb ccc dddd', 3)});
    };
    if ($@) {
	print STDERR "Failed to create/load database table: $@\n";
	return 0;
    }

    return 1;
}

sub write_config {
    my $configfile = shift;

    eval {
	my $config = <<EOF;

    source test_jjs_src {
	type = mysql
	strip_html = 0
	sql_host = $dbhost
	sql_user = $dbuser
	sql_pass = $dbpass
	sql_db = $dbname
	sql_port = $dbport
	sql_query = SELECT * FROM $dbtable
	sql_attr_uint = attr1
    }
    index test_jjs_index {
	source = test_jjs_src
	path = $testdir/test_jjs
	min_word_len = 1
    }
    searchd {
	port = $sph_port
	log = $testdir/searchd.log
	query_log = $testdir/query.log
	pid_file = $pidfile
    }
EOF
    
    open(CONFIG, ">$configfile");
    print CONFIG $config;
    close(CONFIG);
    };
    if ($@) {
	print STDERR "While writing config: $@\n";
	return 0;
    }
    return 1;
}

sub run_indexer {
    my $configfile = shift;

    my $res = `$indexer --config $configfile test_jjs_index`;
    if ($? != 0) {
	print STDERR "Indexer returned $?: $res";
	return 0;
    }
    return 1;
}

sub run_searchd {
    my $configfile = shift;

    my ($pid) = run_forks(sub {
	exec("$searchd --config $configfile");
    });

    my $fp;
    unless (socket($fp, PF_INET, SOCK_STREAM, getprotobyname('tcp'))) {
	print STDERR "Failed to create socket: $!";
	return 0;
    }
    my $dest = sockaddr_in($sph_port, inet_aton("localhost"));
    for (0..3) {
	last if -f "$pidfile";
	sleep(1);
    }
    for (0..3) {
	if (connect($fp, $dest)) {
	    close($fp);
	    return 1;
	}
	else {
	    sleep(1);
	}
    }
    return 0;
}

sub death_handler {
    if (@pids) {
	kill(15, $_) for @pids;
    }
    if ($pidfile) {
	my $pid = $pidfile->slurp;
	kill(15, $pid);
    }
}

sub run_forks {
    my ($forks) = @_;

    my @newpids;
    if ($forks) {
	$forks = [ $forks ] unless (ref($forks) eq "ARRAY");
	for my $f (@{$forks}) {
	    my $pid = fork();
	    die "Fork failed: $!" unless defined $pid;
	    if ($pid == 0) {
		@pids = ();	# prevent child from killing siblings
		# Child process
		if (ref($f) eq "CODE") {
		    &$f;
		}
		else {
		    print STDERR "Don't know how to run test $f\n";
		}
		exit(0);
	    }
	    # Push PID for killing.
	    push(@pids, $pid);
	    push(@newpids, $pid);
	}

	$SIG{INT} = \&death_handler;
	$SIG{KILL} = \&death_handler;
	$SIG{TERM} = \&death_handler;
	$SIG{QUIT} = \&death_handler;
    }
    return @newpids;
}


END { 
    death_handler();
}

