package Sphinx::Search;

use warnings;
use strict;
use Carp;
use Socket;
use base 'Exporter';

=head1 NAME

Sphinx::Search - Sphinx search engine API Perl client

=head1 VERSION

Version 0.01 for Sphinx 0.9.8-20070818

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

    use Sphinx::Search;

    $sphinx = Sphinx::Search->new();

    $results = $sphinx->SetMatchMode(SPH_MATCH_ALL)
                      ->SetSortMode(SPH_SORT_RELEVANCE)
                      ->Query("search terms");

=head1 DESCRIPTION

This is the Perl API client for the Sphinx open-source SQL full-text indexing
search engine, L<http://www.sphinxsearch.com>.

=cut

# Constants to export.
our @EXPORT = qw(	
		SPH_MATCH_ALL SPH_MATCH_ANY SPH_MATCH_PHRASE SPH_MATCH_BOOLEAN SPH_MATCH_EXTENDED
		SPH_SORT_RELEVANCE SPH_SORT_ATTR_DESC SPH_SORT_ATTR_ASC SPH_SORT_TIME_SEGMENTS
		SPH_SORT_EXTENDED
		SPH_GROUPBY_DAY SPH_GROUPBY_WEEK SPH_GROUPBY_MONTH SPH_GROUPBY_YEAR SPH_GROUPBY_ATTR
		SPH_GROUPBY_ATTRPAIR
		);

# known searchd commands
use constant SEARCHD_COMMAND_SEARCH	=> 0;
use constant SEARCHD_COMMAND_EXCERPT	=> 1;
use constant SEARCHD_COMMAND_UPDATE	=> 2;

# current client-side command implementation versions
use constant VER_COMMAND_SEARCH		=> 0x10C;
use constant VER_COMMAND_EXCERPT	=> 0x100;
use constant VER_COMMAND_UPDATE	        => 0x100;

# known searchd status codes
use constant SEARCHD_OK			=> 0;
use constant SEARCHD_ERROR		=> 1;
use constant SEARCHD_RETRY		=> 2;
use constant SEARCHD_WARNING		=> 3;

# known match modes
use constant SPH_MATCH_ALL		=> 0;
use constant SPH_MATCH_ANY		=> 1;
use constant SPH_MATCH_PHRASE		=> 2;
use constant SPH_MATCH_BOOLEAN		=> 3;
use constant SPH_MATCH_EXTENDED		=> 4;

# known sort modes
use constant SPH_SORT_RELEVANCE		=> 0;
use constant SPH_SORT_ATTR_DESC		=> 1;
use constant SPH_SORT_ATTR_ASC		=> 2;
use constant SPH_SORT_TIME_SEGMENTS	=> 3;
use constant SPH_SORT_EXTENDED	=> 4;

# known attribute types
use constant SPH_ATTR_INTEGER		=> 1;
use constant SPH_ATTR_TIMESTAMP		=> 2;
use constant SPH_ATTR_MULTI		=> 0x40000000;

# known grouping functions
use constant SPH_GROUPBY_DAY		=> 0;
use constant SPH_GROUPBY_WEEK		=> 1;
use constant SPH_GROUPBY_MONTH		=> 2;
use constant SPH_GROUPBY_YEAR		=> 3;
use constant SPH_GROUPBY_ATTR		=> 4;
use constant SPH_GROUPBY_ATTRPAIR	=> 5;


=head1 CONSTRUCTOR

=head2 new

    $sph = Sphinx::Search->new;
    $sph = Sphinx::Search->new(\%options);

Create a new Sphinx::Search instance.

OPTIONS

=over 4

=item log

Specify an optional logger instance.  This can be any class that provides error,
warn, info, and debug methods (e.g. see L<Log::Log4perl>).  Logging is disabled
if no logger instance is provided.

=item debug

Debug flag.  If set (and a logger instance is specified), debugging messages
will be generated.

=back

=cut

# create a new client object and fill defaults
sub new {
    my ($class, $options) = @_;
    my $self = {
	_host		=> 'localhost',
	_port		=> 3312,
	_offset		=> 0,
	_limit		=> 20,
	_mode		=> SPH_MATCH_ALL,
	_weights	=> [],
	_sort		=> SPH_SORT_RELEVANCE,
	_sortby		=> "",
	_min_id		=> 0,
	_max_id		=> 0xFFFFFFFF,
	_min		=> {},
	_max		=> {},
	_filter		=> {},
	_groupby	=> "",
	_groupdistinct	=> "",
	_groupfunc	=> SPH_GROUPBY_DAY,
	_groupsort      => '@group desc',
	_maxmatches	=> 1000,
	_cutoff         => 0,
	_retrycount     => 0,
	_retrydelay     => 0,
	_error		=> '',
	_warning	=> '',
	_connection     => undef,
	_persistent_connection => 0,
    };
    bless($self, $class);

    # These options are supported in the constructor, but not recommended 
    # since there is no validation.  Use the Set* methods instead.
    my %legal_opts = map { $_ => 1 } qw/host port offset limit mode weights sort sortby groupby groupbyfunc maxmatches cutoff retrycount retrydelay log debug/;
    for my $opt (keys %$options) {
	$self->{'_' . $opt} = $options->{$opt} if $legal_opts{$opt};
    }
    # Disable debug unless we have something to log to
    $self->{_debug} = 0 unless $self->{_log};

    return $self;
}

=head1 METHODS

=cut

sub _Error {
    my ($self, $msg) = @_;

    $self->{_error} = $msg;
    $self->{_log}->error($msg) if $self->{_log};
}

=head2 GetLastError

    $error = $sph->GetLastError;

Get last error message (string)

=cut

sub GetLastError {
	my $self = shift;
	return $self->{_error};
}

sub _Warning {
    my ($self, $msg) = @_;

    $self->{_warning} = $msg;
    $self->{_log}->warn($msg) if $self->{_log};
}

=head2 GetLastWarning

    $warning = $sph->GetLastWarning;

Get last warning message (string)

=cut

sub GetLastWarning {
	my $self = shift;
	return $self->{_warning};
}

# Persistent connections not currently supported by searchd.
#=head2 SetPersistentConnection
#
#    $sphinx->SetPersistentConnection(1);
#    $sphinx->SetPersistentConnection(0);
#
#Enable/disable persistent connections.
#
#When enabled, the connection to the searchd server is kept open; you must close
#it manually using Disconnect when you have finished with it.  This is the most
#efficient way to reach the search server if you are performing multiple queries
#with the one Sphinx::Search instance, such as in a web application server
#environment.
#
#When disabled, the connection is established and automatically disconnected on
#each query.  This is useful for CGI type environments where you have only one
#request for the lifetime of the process.
#
#=cut
#
sub _SetPersistentConnection {
    my $self = shift;
    $self->{_persistent_connection} = shift;
    return $self;
}
#
#=head2 Disconnect
#
#    $sph->Disconnect;
#
#Disconnect from the search server.  See L<SetPersistentConnection> for a description.
#
#=cut
#
sub _Disconnect {
    my $self = shift;

    if ($self->{_connection}) {
	close($self->{_connection});
	$self->{_connection} = undef;
    }
    return $self;
}


#-------------------------------------------------------------

# connect to searchd server

sub _Connect {
	my $self = shift;
	
	my $debug = $self->{_debug};

	$self->{_log}->debug("Connecting to $self->{_host}:$self->{_port}") if $debug;

	return $self->{_connection} if $self->{_connection};

	# connect socket
	my $fp;
	socket($fp, PF_INET, SOCK_STREAM, getprotobyname('tcp')) || Carp::croak("socket: ".$!);
	binmode($fp, ':bytes');
	my $dest = sockaddr_in($self->{_port}, inet_aton($self->{_host}));
	connect($fp, $dest);
	
	if($!) {
		$self->_Error("connection to {$self->{_host}}:{$self->{_port}} failed: $!");
		return 0;
	}	
	
	# check version
	my $buf = '';
	recv($fp, $buf, 4, 0) eq "" || croak("recv: ".$!);
	my $v = unpack("N*", $buf);
	$v = int($v);
	if($v < 1) {
		close($fp) || croak("close: $!");
		$self->_Error("expected searchd protocol version 1+, got version '$v'");
	}

	$self->{_log}->debug("Sending version") if $debug;

	# All ok, send my version
	send($fp, pack("N", 1),0);

	$self->{_connection} = $fp;

	$self->{_log}->debug("Connection complete") if $debug;

	return $fp;
}

#-------------------------------------------------------------

# get and check response packet from searchd server
sub _GetResponse {
	my $self = shift;
	my $fp = shift;
	my $client_ver = shift;

	my $header;
	recv($fp, $header, 8, 0) eq "" || croak("recv: ".$!);

	my ($status, $ver, $len ) = unpack("n2N", $header);
        my ($chunk, $response);
	while(defined($chunk = <$fp>)) {
		$response .= $chunk;
	}
        $self->_Disconnect unless $self->{_persistent_connection};

	# check response
        if ( !$response || length($response) != $len ) {
		$self->_Error( $len 
			? "failed to read searchd response (status=$status, ver=$ver, len=$len, read=". length($response) . ")"
       			: "received zero-sized searchd response");
		return 0;
	}

	# check status
        if ( $status==SEARCHD_WARNING ) {
	    my ($wlen) = unpack ( "N*", substr ( $response, 0, 4 ) );
	    $self->_Warning(substr ( $response, 4, $wlen ));
	    return substr ( $response, 4+$wlen );
	}
        if ( $status==SEARCHD_ERROR ) {
		$self->_Error("searchd error: " . substr ( $response, 4 ));
		return 0;
	}
	if ( $status==SEARCHD_RETRY ) {
		$self->_Error("temporary searchd error: " . substr ( $response, 4 ));
                return 0;
        }
        if ( $status!=SEARCHD_OK ) {
        	$self->_Error("unknown status code '$status'");
        	return 0;
	}

	# check version
        if ( $ver<$client_ver ) {
	    $self->_Warning(sprintf ( "searchd command v.%d.%d older than client's v.%d.%d, some options might not work",
				      $ver>>8, $ver&0xff, $client_ver>>8, $client_ver&0xff ));
	}
        return $response;
}

=head2 SetServer

    $sph->SetServer($host, $port);

Set the host/port details for the searchd server.  Returns $sph.

=cut

sub SetServer {
    my $self = shift;
    my $host = shift;
    my $port = shift;

    croak("host is not defined") unless defined($host);
    croak("port is not defined") unless defined($port);

    $self->{_host} = $host;
    $self->{_port} = $port;
    return $self;
}

=head2 SetLimits

    $sph->SetLimits($offset, $limit);
    $sph->SetLimits($offset, $limit, $max);

Set match offset/limits, and optionally the max number of matches to return.

Returns $sph.

=cut

sub SetLimits {
        my $self = shift;
        my $offset = shift;
        my $limit = shift;
	my $max = shift || 0;
        croak("offset should be an integer >= 0") unless ($offset =~ /^\d+$/ && $offset >= 0) ;
        croak("limit should be an integer >= 0") unless ($limit =~ /^\d+$/ && $limit >= 0);
        $self->{_offset} = $offset;
        $self->{_limit}  = $limit;
	if($max > 0) {
		$self->{_maxmatches} = $max;
	}
	return $self;
}

=head2 SetMatchMode

    $sph->SetMatchMode($mode);

Set match mode, which may be one of:

=over 4

=item SPH_MATCH_ALL

Match all words

=item SPH_MATCH_ANY		

Match any words

=item SPH_MATCH_PHRASE	

Exact phrase match

=item SPH_MATCH_BOOLEAN	

Boolean match, using AND (&), OR (|), NOT (!,-) and parenthetic grouping.

=item SPH_MATCH_EXTENDED	

Extended match, which includes the Boolean syntax plus field, phrase and
proximity operators.

=back

Returns $sph.

=cut

sub SetMatchMode {
        my $self = shift;
        my $mode = shift;
        croak("Match mode not defined") unless defined($mode);
        croak("Unknown matchmode: $mode") unless ( $mode==SPH_MATCH_ALL || $mode==SPH_MATCH_ANY || $mode==SPH_MATCH_PHRASE || $mode==SPH_MATCH_BOOLEAN || $mode==SPH_MATCH_EXTENDED );
        $self->{_mode} = $mode;
	return $self;
}

=head2 SetSortMode

    $sph->SetSortMode(SPH_SORT_RELEVANCE);
    $sph->SetSortMode($mode, $sortby);

Set sort mode, which may be any of:

=over 4

=item SPH_SORT_RELEVANCE - sort by relevance

=item SPH_SORT_ATTR_DESC, SPH_SORT_ATTR_ASC

Sort by attribute descending/ascending.  $sortby specifies the sorting attribute.

=item SPH_SORT_TIME_SEGMENTS

Sort by time segments (last hour/day/week/month) in descending order, and then
by relevance in descending order.  $sortby specifies the time attribute.

=item SPH_SORT_EXTENDED

Sort by SQL-like syntax.  $sortby is the sorting specification.

=back

Returns $sph.

=cut

sub SetSortMode {
        my $self = shift;
        my $mode = shift;
	my $sortby = shift || "";
        croak("Sort mode not defined") unless defined($mode);
        croak("Unknown sort mode: $mode") unless ( $mode==SPH_SORT_RELEVANCE || 
						   $mode==SPH_SORT_ATTR_DESC ||
						   $mode==SPH_SORT_ATTR_ASC || 
						   $mode==SPH_SORT_TIME_SEGMENTS ||
						   $mode==SPH_SORT_EXTENDED
						   );
	croak("Sortby must be defined") unless ($mode==SPH_SORT_RELEVANCE || length($sortby));
        $self->{_sort} = $mode;
	$self->{_sortby} = $sortby;
	return $self;
}

=head2 SetWeights
    
    $sph->SetWeights([ 1, 2, 3, 4]);

Set per-field (integer) weights.  The ordering of the weights correspond to the
ordering of fields as indexed.

Returns $sph.

=cut

sub SetWeights {
        my $self = shift;
        my $weights = shift;
        croak("Weights is not an array reference") unless (ref($weights) eq 'ARRAY');
        foreach my $weight (@$weights) {
                croak("Weight: $weight is not an integer") unless ($weight =~ /^\d+$/);
        }
        $self->{_weights} = $weights;
	return $self;
}

=head2 SetIDRange

    $sph->SetIDRange($min, $max);

Set IDs range only match those records where document ID
is between $min and $max (including $min and $max)

Returns $sph.

=cut

sub SetIDRange {
	my $self = shift;
	my $min = shift;
	my $max = shift;
	croak("min_id is not an integer") unless ($min =~ /^\d+$/);
	croak("max_id is not an integer") unless ($max =~ /^\d+$/);
	croak("min_id is larger than or equal to max_id") unless ($min < $max);
	$self->{_min_id} = $min;
	$self->{_max_id} = $max;
	return $self;
}

=head2 SetFilter

    $sph->SetFilter($attr, \@values);

Sets the results to be filtered on the given attribute.  Only results which have
attributes matching the given values will be returned.

This may be called multiple times with different attributes to select on
multiple attributes.

Returns $sph.

=cut

sub SetFilter {
        my $self = shift;
	my $attribute = shift;
	my $values = shift;
	croak("attribute is not defined") unless (defined $attribute);
	croak("values is not an array reference") unless (ref($values) eq 'ARRAY');
	croak("values reference is empty") unless (scalar(@$values));

	foreach my $value (@$values) {
		croak("value $value is not an integer") unless ($value =~ /^\d+$/);
	}
	$self->{_filter}{$attribute} = $values;
	return $self;
}

=head2 SetFilterRange

    $sph->SetFilterRange($attr, $min, $max);

Sets the results to be filtered on a range of values for the given
attribute. Only those records where $attr column value is between $min and $max
(including $min and $max) will be returned.

Returns $sph.

=cut

sub SetFilterRange {
	my $self = shift;
	my $attribute = shift;
	my $min = shift;
	my $max = shift;
	croak("attribute is not defined") unless (defined $attribute);
	croak("min: $min is not an integer") unless ($min =~ /^\d+$/);
	croak("max: $max is not an integer") unless ($max =~ /^\d+$/);
	croak("min value should be <= max") unless ($min <= $max);
	
	$self->{_min}{$attribute} = $min;
	$self->{_max}{$attribute} = $max;
	return $self;
}

=head2 SetGroupBy

    $sph->SetGroupBy($attr, $func);
    $sph->SetGroupBy($attr, $func, $groupsort);

Sets attribute and function of results grouping.

In grouping mode, all matches are assigned to different groups based on grouping
function value. Each group keeps track of the total match count, and the best
match (in this group) according to current sorting function. The final result
set contains one best match per group, with grouping function value and matches
count attached.

$attr is any valid attribute.

$func is one of:

=over 4

=item SPH_GROUPBY_DAY

Group by day (assumes timestamp type attribute of form YYYYMMDD)

=item SPH_GROUPBY_WEEK

Group by week (assumes timestamp type attribute of form YYYYNNN)

=item SPH_GROUPBY_MONTH

Group by month (assumes timestamp type attribute of form YYYYMM)

=item SPH_GROUPBY_YEAR

Group by year (assumes timestamp type attribute of form YYYY)

=item SPH_GROUPBY_ATTR

Group by attribute value

=item SPH_GROUPBY_ATTRPAIR

Group by two attributes, being the given attribute and the attribute that
immediately follows it in the sequence of indexed attributes.  The specified
attribute may therefore not be the last of the indexed attributes.

=back

Groups in the set of results can be sorted by any SQL-like sorting clause,
including both document attributes and the following special internal Sphinx
attributes:

=over 4

=item @id - document ID;

=item @weight, @rank, @relevance -  match weight;

=item @group - group by function value;

=item @count - number of matches in group.

=back

The default mode is to sort by groupby value in descending order,
ie. by "@group desc".

In the results set, "total_found" contains the total amount of matching groups
over the whole index.

WARNING: grouping is done in fixed memory and thus its results
are only approximate; so there might be more groups reported
in total_found than actually present. @count might also
be underestimated. 

For example, if sorting by relevance and grouping by a "published"
attribute with SPH_GROUPBY_DAY function, then the result set will
contain only the most relevant match for each day when there were any
matches published, with day number and per-day match count attached,
and sorted by day number in descending order (ie. recent days first).

=cut

sub SetGroupBy {
	my $self = shift;
	my $attribute = shift;
	my $func = shift;
	my $groupsort = shift || '@group desc';
	croak("attribute is not defined") unless (defined $attribute);
	croak("Unknown grouping function: $func") unless ($func==SPH_GROUPBY_DAY
							  || $func==SPH_GROUPBY_WEEK
							  || $func==SPH_GROUPBY_MONTH
							  || $func==SPH_GROUPBY_YEAR
							  || $func==SPH_GROUPBY_ATTR
							  || $func==SPH_GROUPBY_ATTRPAIR
							  );

	$self->{_groupby} = $attribute;
	$self->{_groupfunc} = $func;
	$self->{_groupsort} = $groupsort;
	return $self;
}

=head2 SetGroupDistinct

    $sph->SetGroupDistinct($attr);

Set count-distinct attribute for group-by queries

=cut

sub SetGroupDistinct {
    my $self = shift;
    my $attribute = shift;
    croak("attribute is not defined") unless (defined $attribute);
    $self->{_groupdistinct} = $attribute;
    return $self;
}

=head2 SetRetries

    $sph->SetRetries($count, $delay);

Set distributed retries count and delay

=cut

sub SetRetries {
    my $self = shift;
    my $count = shift;
    my $delay = shift || 0;

    croak("count: $count is not an integer >= 0") unless ($count =~ /^\d+$/o && $count >= 0);
    croak("delay: $delay is not an integer >= 0") unless ($delay =~ /^\d+$/o && $delay >= 0);
    $self->{_retrycount} = $count;
    $self->{_retrydelay} = $delay;
    return $self;
}


=head2 Query

    $results = $sphinx->Query($query, $index);

Connect to searchd server and run given search query.

=over 4

=item query is query string

=item index is index name to query, default is "*" which means to query all indexes.  Use a space or comma separated list to search multiple indexes.

=back

Returns undef on failure

Returns hash which has the following keys on success:

=over 4

=item matches
    
Array containing hashes with found documents ( "doc", "weight", "group", "stamp" )
 
=item total

Total amount of matches retrieved (upto SPH_MAX_MATCHES, see sphinx.h)

=item total_found
                    
Total amount of matching documents in index
 
=item time
          
Search time

=item words
           
Hash which maps query terms (stemmed!) to ( "docs", "hits" ) hash

=back

=cut

sub Query {
    my $self = shift;
    my $query = shift;
    my $index = shift || '*';

    my $fp = $self->_Connect() or return undef;

    ##################
    # build request
    ##################

    my $req;
    $req = pack ( "NNNN", $self->{_offset}, $self->{_limit}, $self->{_mode}, $self->{_sort} ); # mode and limits
    $req .= pack ( "N", length($self->{_sortby}) ) . $self->{_sortby};
    $req .= pack ( "N", length($query) ) . $query; # query itself
    $req .= pack ( "N", scalar(@{$self->{_weights}}) );	# weights
    foreach my $weight (@{$self->{_weights}}) {
	$req .= pack ( "N", int($weight));
    }
    $req .= pack ( "N", length($index) ) . $index; # indexes
    $req .=	pack ( "NNN", 0, int($self->{_min_id}), int($self->{_max_id}) ); # id32 range

    # filters
    $req .= pack ( "N", scalar(keys %{$self->{_min}}) + scalar(keys %{$self->{_filter}}) );

    foreach my $attr (keys %{$self->{_min}}) {
	$req .= 
	    pack ( "N", length($attr) ) . $attr .
	    pack ( "NNNN", 3, 0, $self->{_min}{$attr}, $self->{_max}{$attr} );
    }

    foreach my $attr (keys %{$self->{_filter}}) {
	my $values = $self->{_filter}{$attr};
	$req .=
	    pack ( "N", length($attr) ) . $attr .
	    pack ( "N*", scalar(@$values), @$values );
    }

    # group-by clause, max-matches count, group-sort clause, cutoff count
    $req .= pack ( "NN", $self->{_groupfunc}, length($self->{_groupby}) ) . $self->{_groupby};
    $req .= pack ( "N", $self->{_maxmatches} );
    $req .= pack ( "N", length($self->{_groupsort}) ) . $self->{_groupsort};
    $req .= pack ( "NNN", $self->{_cutoff}, $self->{_retrycount}, $self->{_retrydelay} );
    $req .= pack ( "N", length($self->{_groupdistinct}) ) . $self->{_groupdistinct};

    ##################
    # send query, get response
    ##################

    my $len = length($req);
    $req = pack ( "nnN", SEARCHD_COMMAND_SEARCH, VER_COMMAND_SEARCH, $len ) . $req; # add header
    send($fp, $req ,0);

    my $response = $self->_GetResponse ( $fp, VER_COMMAND_SEARCH );
    return undef unless $response;

    ##################
    # parse response
    ##################

    my $result = {};		# Empty hash ref
    $result->{matches} = [];	# Empty array ref
    my $max = length($response); # Protection from broken response

    # read schema
    my $p = 0;
    my @fields;
    my (%attrs, @attr_list);

    my $nfields = unpack ( "N", substr ( $response, $p, 4 ) ); $p += 4;
    while ( $nfields-->0 && $p<$max ) {
	my $len = unpack ( "N", substr ( $response, $p, 4 ) ); $p += 4;
	push(@fields, substr ( $response, $p, $len )); $p += $len;
    }
    $result->{"fields"} = \@fields;

    my $nattrs = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
    while ( $nattrs-->0 && $p<$max  ) {
	my $len = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
	my $attr = substr ( $response, $p, $len ); $p += $len;
	my $type = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
	$attrs{$attr} = $type;
	push(@attr_list, $attr);
    }
    $result->{"attrs"} = \%attrs;

    # read match count
    my $count = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
    my $id64 = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;

    # read matches
    while ( $count-->0 && $p<$max ) {
	my $data = {};
	if ($id64) {
	    ( $data->{dochi}, $data->{doclo}, $data->{weight} ) = unpack("N*N*N*", substr($response,$p,12));
	    $data->{doc} = ($data->{dochi} << 32) + $data->{doclo};
	    $p += 12;
	}
	else {
	    ( $data->{doc}, $data->{weight} ) = unpack("N*N*", substr($response,$p,8));
	    $p += 8;
	}
	foreach my $attr (@attr_list) {
	    my $val = unpack ( "N*", substr ( $response, $p, 4 ) ); $p += 4;
	    if ($attrs{$attr} & SPH_ATTR_MULTI) {
		my $nvalues = $val;
		$data->{$attr} = [];
		while ($nvalues-->0 && $p < $max) {
		    $val = unpack( "N*", substr ( $response, $p, 4 ) ); $p += 4;
		    push(@{$data->{$attr}}, $val);
		}
	    }
	    else {
		$data->{$attr} = $val;
	    }
	}
	push(@{$result->{matches}}, $data);
    }
    my $words;
    ($result->{total}, $result->{total_found}, $result->{time}, $words) = unpack("N*N*N*N*", substr($response, $p, 16));
    $result->{time} = sprintf ( "%.3f", $result->{"time"}/1000 );
    $p += 16;

    while ( $words-->0 && $p < $max) {
	my $len = unpack ( "N*", substr ( $response, $p, 4 ) ); 
	$p += 4;
	my $word = substr ( $response, $p, $len ); 
	$p += $len;
	my ($docs, $hits) = unpack ("N*N*", substr($response, $p, 8));
	$p += 8;
	$result->{words}{$word} = {
	    "docs" => $docs,
	    "hits" => $hits
	    };
    }
    return $result;
}

=head2 BuildExcerpts

    $excerpts = $sph->BuildExcerpts($docs, $index, $words, $opts)

Generation document excerpts for the specified documents.

=over 4

=item docs 

An array reference of strings which represent the document
contents

=item index 

A string specifiying the index whose settings will be used
for stemming, lexing and case folding

=item words 

A string which contains the words to highlight

=item opts 

A hash which contains additional optional highlighting parameters:

=over 4

=item before_match - a string to insert before a set of matching words, default is "<b>"
=item after_match - a string to insert after a set of matching words, default is "<b>"

=item chunk_separator - a string to insert between excerpts chunks, default is " ... "

=item limit - max excerpt size in symbols (codepoints), default is 256

=item around - how many words to highlight around each match, default is 5

=back

=back

Returns undef on failure.

Returns an array of string excerpts on success.

=cut

sub BuildExcerpts {
	my ($self, $docs, $index, $words, $opts) = @_;
	$opts ||= {};
	croak("BuildExcepts() called with incorrect parameters") 
	    unless (ref($docs) eq 'ARRAY' 
		    && defined($index) 
		    && defined($words) 
		    && ref($opts) eq 'HASH');
        my $fp = $self->_Connect() or return undef;

	##################
	# fixup options
	##################
	$opts->{"before_match"} ||= "<b>";
	$opts->{"after_match"} ||= "</b>";
	$opts->{"chunk_separator"} ||= " ... ";
	$opts->{"limit"} ||= 256;
	$opts->{"around"} ||= 5;

	##################
	# build request
	##################

	# v.1.0 req
	my $req;
	$req = pack ( "NN", 0, 1 ); # mode=0, flags=1 (remove spaces)
	$req .= pack ( "N", length($index) ) . $index; # req index
	$req .= pack ( "N", length($words) ) . $words; # req words

	# options
	$req .= pack ( "N", length($opts->{"before_match"}) ) . $opts->{"before_match"};
	$req .= pack ( "N", length($opts->{"after_match"}) ) . $opts->{"after_match"};
	$req .= pack ( "N", length($opts->{"chunk_separator"}) ) . $opts->{"chunk_separator"};
	$req .= pack ( "N", int($opts->{"limit"}) );
	$req .= pack ( "N", int($opts->{"around"}) );

	# documents
	$req .= pack ( "N", scalar(@$docs) );
	foreach my $doc (@$docs) {
		croak('BuildExcepts: Found empty document in $docs') unless ($doc);
		$req .= pack("N", length($doc)) . $doc;
	}

	##########################
        # send query, get response
	##########################

	my $len = length($req);
	$req = pack ( "nnN", SEARCHD_COMMAND_EXCERPT, VER_COMMAND_EXCERPT, $len ) . $req; # add header
	send($fp, $req ,0);
	
	my $response = $self->_GetResponse($fp, VER_COMMAND_EXCERPT);
	return 0 unless ($response);

	my ($pos, $i) = 0;
	my $res = [];	# Empty hash ref
        my $rlen = length($response);
        for ( $i=0; $i< scalar(@$docs); $i++ ) {
		my $len = unpack ( "N*", substr ( $response, $pos, 4 ) );
		$pos += 4;

                if ( $pos+$len > $rlen ) {
			$self->_Error("incomplete reply");
			return 0;
		}
		push(@$res, substr ( $response, $pos, $len ));
		$pos += $len;
        }
        return $res;
}


=head2 UpdateAttributes

    $sph->UpdateAttributes($index, \@attrs, \%values);

Update specified attributes on specified documents

=over 4

=item index 

Name of the index to be updated

=item attrs 

Array of attribute name strings

=item values 

A hash with key as document id, value as an array of new attribute values

=back

Returns number of actually updated documents (0 or more) on success

Returns undef on failure

Usage example:

 $sph->UpdateAttributes("test1", [ qw/group_id/ ], { 1 => [ 456] }) );

=cut

sub UpdateAttributes  {
    my ($self, $index, $attrs, $values ) = @_;

    croak("index is not defined") unless (defined $index);
    croak("attrs must be an array") unless ref($attrs) eq "ARRAY";
    for my $attr (@$attrs) {
	croak("attribute is not defined") unless (defined $attr);
    }
    croak("values must be a hashref") unless ref($values) eq "HASH";

    for my $id (keys %$values) {
	my $entry = $values->{$id};
	croak("value id $id is not an integer") unless ($id =~ /^(\d+)$/o);
	croak("value entry must be an array") unless ref($entry) eq "ARRAY";
	croak("size of values must match size of attrs") unless @$entry == @$attrs;
	for my $v (@$entry) {
	    croak("entry value $v is not an integer") unless ($v =~ /^(\d+)$/o);
	}
    }

    ## build request
    my $req = pack ( "N", length($index) ) . $index;

    $req .= pack ( "N", scalar @$attrs );
    for my $attr (@$attrs) {
	$req .= pack ( "N", length($attr) ) . $attr;
    }
    $req .= pack ( "N", scalar keys %$values );
    foreach my $id (keys %$values) {
	my $entry = $values->{$id};
	$req .= pack ( "N", $id );
	for my $v ( @$entry ) {
	    $req .= pack ( "N", $v );
	}
    }

    ## connect, send query, get response
    my $fp = $self->_Connect() or return undef;

    my $len = length($req);
    $req = pack ( "nnN", SEARCHD_COMMAND_UPDATE, VER_COMMAND_UPDATE, $len ) . $req; ## add header
    send ( $fp, $req, 0);

    my $response = $self->_GetResponse ( $fp, VER_COMMAND_UPDATE );
    return undef unless $response;

    ## parse response
    my $p = 0;
    my ($updated) = unpack ( "N*", substr ( $response, $p, 4 ) );
    return $updated;
}

=head1 SEE ALSO

L<http://www.sphinxsearch.com>


=head1 AUTHOR

Jon Schutz

=head1 BUGS

Please report any bugs or feature requests to
C<bug-sphinx-search at rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Sphinx-Search>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Sphinx::Search

You can also look for information at:

=over 4

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Sphinx-Search>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Sphinx-Search>

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Sphinx-Search>

=item * Search CPAN

L<http://search.cpan.org/dist/Sphinx-Search>

=back

=head1 ACKNOWLEDGEMENTS

This module is based on Sphinx.pm (not deployed to CPAN) for Sphinx version
0.9.7-rc1, by Len Kranendonk, which was in turn based on the Sphinx PHP API.

=head1 COPYRIGHT & LICENSE

Copyright 2007 Jon Schutz, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License.

=cut


1;
