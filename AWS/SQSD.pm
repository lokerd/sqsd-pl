package AWS::SQSD;

use strict;
use warnings;

use POSIX qw( setsid );
use AWS::CLIWrapper;
use LWP::UserAgent;
use LWP::Protocol::https;
use HTTP::Request::Common;
use JSON;

use Data::Dumper;
# Functions for the SQS Daemon
# lokerd@amazon.com


# AWS::SQSD->new()		: constructor
# AWS::SQSD->get_message()	: retrieve from queue
# AWS::SQSD->process()		: process queue message
# AWS::SQSD->api_call()		: API call via AWS::CLIWrapper

# Regex placeholders
use constant {
	REGION  => '___region___',
	Q       => '___queue___',
	QURL    => '___qurl___',
	ALLOC   => '___alloc___',
	TARGET  => '___target___'
};

# Constructor 	: new({})
# ARGS		: anonymous hash of constructor args - placeholder
# RETURN	: blessed reference 
sub new 
{
	my ( $class, $args ) = @_;
	my $self = {
		queue => +Q,
                queue_url => +QURL,
		region => +REGION,
		eip_status => 'false',
		eip_assoc => "",
		args => $args
	};
	bless( $self, $class );
	$self->_init();
	return $self; 
}

# _init()
# ARGS		: NONE
# RETURN	: $self->{_aws}         - AWS::CLIWrapper object
# 		: $self->{_ua}          - LWP::UserAgent object
sub _init 
{
	my $self = shift;
	eval 
	{
		$self->{ _aws } = AWS::CLIWrapper->new( region => $self->{ region } );
		$self->{ _ua } = LWP::UserAgent->new();
	}; 
	die $@ if $@; 
	$self->daemon();
	return $self;
}   

# get_message()
# ARGS 		: NONE
# RETURN	: anonymous hash $message or undef
sub get_message 
{
	my $self = shift;
	my $message = $self->api_call( 'sqs', 'receive-message', {
			'queue-url' => $self->{queue_url}
	} );
	if ( $message ) 
        { return $message } 
        else 
        { return undef }
}

# rm(str)
# ARGS 		: $handle - sqs message receipt handle
# RETURN	: true if successful, undef otherwise
sub rm 
{
	my ( $self,$handle ) = @_;
	my $res = $self->api_call( 'sqs', 'delete-message', {
			'queue-url' => $self->{ queue_url },
			'receipt-handle' => $handle 
	} );
	if ( $res ) 
        { return $res }
	else 
        { return undef }
}

sub disassociate 
{
	my ( $self, $response ) = @_;
	my $res = $self->api_call( 'ec2', 'disassociate-address', {
		'instance-id' => +TARGET,
		'association-id' => $self->{ eip_assoc }
	} );
        my $return = undef;
	if ( $res->{ return } eq 'true' ) 
	{
		$self->{ eip_status } = 'false';
		$self->{ eip_assoc } = "";
		$return = $self->s3_signal( $response, 'SUCCESS', undef );
	} 
	else { $return = $self->s3_signal( $response, 'FAILURE', "disassociate-address failed" ) }
	if ( $return ) { return $return }
	else { return undef }
}

sub associate 
{
	my ( $self, $response ) = @_;
	my $res = $self->api_call( 'ec2', 'associate-address', {
		'instance-id' => +TARGET,
		'allocation-id' => +ALLOC
	} );
        my $return = undef;
	if ( $res->{ AssociationId } ) 
	{
		$self->{ eip_status } = 'true';
		$self->{ eip_assoc } = $res->{ AssociationId };
		$return = $self->s3_signal( $response, 'SUCCESS', undef );
	} 
	else { $return = $self->s3_signal( $response, 'FAILURE', "associate-address failed" ) }
	if ( $return ) { return $return }
	else { return undef }
}

sub message_decode 
{
	my ( $self, $message ) = @_;
    	my $tmp = decode_json $message;
	my $body = decode_json $tmp->{ Message };
        $self->{ response_url } = $body->{ ResponseURL };
	my $response = {
		'RequestId' => $body->{ RequestId },
		'StackId' => $body->{ StackId },
		'LogicalResourceId' => $body->{ LogicalResourceId },
		'PhysicalResourceId' => 'lokerd'
	};
	if( $response and $body ) { return($response, $body) }
	else { return undef }
}

# process({})
# ARGS		: $message - anonymous message hash
# 		  returned by AWS::SQSD->get_message()  
# RETURN	: true if successful or undef
sub process 
{
	my ( $self, $message ) = @_;
        my ( $response, $body ) = $self->message_decode( $message );
        my $return = undef;
	if ( $body->{ ResourceProperties }{ Attach } eq $self->{ eip_status } ) 
	{ $return = $self->s3_signal( $response, 'SUCCESS', undef ) } 
	else
	{
		if ( $self->{ eip_status } eq 'false' ) 
		{ $return = $self->associate( $response ) } 
		else { $return = $self->disassociate( $response ) }
	}	
	if ( $return ) { return $return }
	else { return undef }
}

# s3_signal({} str)
# ARGS 		: $response - CF response anon hash
#               : $response_url - from CF notification
# RETURN	: true if rc 200, undef otherwise
sub s3_signal
{
	my ( $self, $response, $status, $reason ) = @_;
        if ( $status eq 'FAILED' ) { $self->{Reason} = $reason };  
	$response->{ Status } = $status;
	$response->{ Data }{ AttachmentStatus } = $self->{ eip_status };
	my $json = encode_json( $response );
	my $req = PUT $self->{ response_url };
	$req->header( 'Content-Type' => '' );
	$req->content( $json );
	my $res = $self->{ _ua }->request( $req );
	if ( $res->{ _rc } eq '200' ) { return }
	else { return undef }
}

# api_call(str str {})
# ARGS		: $service - aws cli service eg. [sqs,autoscaling,sns...]
# 		: $operation - cli operation eg. [delete-message,create-queue,...]
# 		: $params - anonymous hash of option/argument pairs
# 		  $params = { 'option' => '$argument', 'queue-url' => '$q' }
# RETURN	: $res - anonymous hash of the response or undef
sub api_call {
	my ( $self, $service, $operation, $params ) = @_;
	my $res = $self->{ _aws }->$service( $operation, $params );
	if ( $res ) { return $res }
	else 
	{
		warn $AWS::CLIWrapper::Error->{ Code };
		warn $AWS::CLIWrapper::Error->{ Message };
		return undef;
	}
}

# daemon()
# ARGS		: NONE
# RETURN	: NONE
# STDOUT, STDERR: /var/log/sqsd{-error}.log
sub daemon {
	chdir '/' or die "Can't chdir to /: $!";
	open STDIN, '/dev/null' or die "Can't read /dev/null: $!";
	open STDOUT, '>>/var/log/sqsd.log' or die "Can't write to /var/log/sqsd.log: $!";
	open STDERR, '>>/var/log/sqsd-error.log' or die "Can't write to /var/log/sqsd-error.log: $!";
	defined(my $pid = fork) or die "Can't fork: $!";
	exit if $pid;
	setsid or die "Can't start a new session: $!";
	umask 0;
}

# Debug		: debug(string)
# ARGS		: $debug_string - string to be printed
# RETURNS	: NONE
sub debug {
	my $debug_string = shift;
	printf( "%.3f :: %-12s\n", time, $debug_string );
}

1;
