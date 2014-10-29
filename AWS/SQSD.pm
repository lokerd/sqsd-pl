package AWS::SQSD;

use strict;
use warnings;

use POSIX qw(setsid);
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
	my ($class, $args) = @_;
	my $self = {
		queue => +Q,
                queue_url => +QURL,
		region => +REGION,
		eip_status => 'false',
		eip_assoc => "",
		_debug => 1,
		_physical_id => &id_gen(),
		args => $args
	};
	bless($self, $class);
	$self->_init();
	return $self; 
}

sub id_gen
{
        my @chars = ("A".."Z", "a".."z");
        my $string;
        $string .= $chars[rand @chars] for 1..8;
        return $string;
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
		$self->{_aws} = AWS::CLIWrapper->new(region => $self->{region});
		$self->{_ua} = LWP::UserAgent->new();
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
	my $message = $self->api_call('sqs', 'receive-message', {
			'queue-url' => $self->{queue_url}
	});
	if ($message) { return $message } else { return undef }
}

# rm(str)       : delete message from SQS queue
# ARGS 		: $handle - sqs message receipt handle
# RETURN	: true if successful, undef otherwise
sub rm 
{
	my ($self,$handle) = @_;
	my $res = $self->api_call('sqs', 'delete-message', {
			'queue-url' => $self->{queue_url},
			'receipt-handle' => $handle 
	});
	if ($res) { return $res } else { return undef }
}

# disassociate(): disassociate EIP from target
# ARGS		: $response - anon hash containing respone
# RETURN	: true for signal success, undef for signal or api call fail
sub disassociate 
{
	my ($self, $type, $response_url, $response) = @_;
	my $res = $self->api_call('ec2', 'disassociate-address', {
		'association-id' => $self->{eip_assoc}
	} );
        my $return = undef;
	if ($res->{return} eq 'true') 
	{
		$self->{eip_status} = 'false';
		$self->{eip_assoc} = "";
		$return = $self->s3_signal($type, $response_url, $response, 'SUCCESS', undef);
	} 
	else { $return = $self->s3_signal($type, $response_url, $response, 'FAILED', "disassociate-address failed") }
	if ($return) { return $return } else { return undef }
}

# associate()   : associate EIP to target
# ARGS		: $response - anon hash containing respone
# RETURN	: true for signal success, undef for signal or api call fail
sub associate 
{
	my ($self, $type, $response_url, $response) = @_;
	my $res = $self->api_call('ec2', 'associate-address', {
		'instance-id' 		=> +TARGET,
		'allocation-id' 	=> +ALLOC,
		'allow_reassociation'   => $AWS::CLIWrapper::true
	});
        my $return = undef;
	if ($res->{AssociationId}) 
	{
		$self->{eip_status} = 'true';
		$self->{eip_assoc} = $res->{AssociationId};
		$return = $self->s3_signal($type, $response_url, $response, 'SUCCESS', undef);
	} 
	else { $return = $self->s3_signal($type, $response_url, $response, 'FAILED', "associate-address failed") }
	if ($return) { return $return } else { return undef }
}

# message_decode()      : start response hash and decode SQS message
# ARGS		        : $message - popped from SQS queue 
# RETURN	        : $response - anon hash of response details
#                       : $body - anon hash of message body
sub message_decode 
{
	my ($self, $message) = @_; my $attach;
	my $tmp = decode_json $message; my $body = decode_json $tmp->{Message};
	if ($body->{RequestType} eq 'Delete') { $attach = 'false' }
	else { $attach = $body->{ResourceProperties}{Attach} }
	$self->{_physical_id} = &id_gen() if $body->{RequestType} eq 'Delete';
	my $response = {
		'RequestId' => $body->{RequestId},
		'StackId' => $body->{StackId},
		'LogicalResourceId' => $body->{LogicalResourceId},
		'PhysicalResourceId' => $self->{_physical_id}
	};
	if ($body) 
	{ return ($body->{RequestType}, $body->{ResponseURL}, $response, $attach) } 
	else { return undef }
}

# process({})           : process stack notification
# ARGS		        : $message - anonymous message hash
# 		          returned by AWS::SQSD->get_message()  
# RETURN	        : true if successful or undef
sub process 
{
	my ($self, $message) = @_; my $return = undef;
        my ($type, $response_url, $response, $attach) = $self->message_decode($message);
	if ($attach eq $self->{eip_status}) 
	{ $return = $self->s3_signal($type, $response_url, $response, 'SUCCESS', undef) } 
	else
	{
		if ($self->{eip_status} eq 'false') 
		{ $return = $self->associate($type, $response_url, $response) } 
		else 
                { $return = $self->disassociate($type, $response_url, $response) }
	}	
	if ($return) { return $return } else { return undef }
}

# s3_signal({} str)     : send encoded response hash to CF provided URL
# ARGS 		        : $response - CF response anon hash
#                       : $status - 'SUCCESS' or 'FAILED'
#                       : $reason - only needed for 'FAILED'
# RETURN	        : true if rc 200, undef otherwise
sub s3_signal
{
	my ($self, $type, $response_url, $response, $status, $reason) = @_;
        if ($status eq 'FAILED') 
	{ 
		$response->{Reason} = $reason;
		delete $response->{PhysicalResourceId} if $type eq 'Create'; 
	}
	$response->{Status} = $status;
	my $json = encode_json($response);
        debug($json) if $self->{_debug} == 1;
	my $req = PUT $response_url;
	$req->header('Content-Type' => '');
	$req->content($json);
	my $res = $self->{_ua}->request($req);
        debug($res) if $self->{_debug} == 1;
	if ($res->{_rc} eq '200') { return } else { return undef }
}

# api_call(str str {})  : AWS API call via AWS::CLIWrapper
# ARGS		        : $service - aws cli service eg. [sqs,autoscaling,sns...]
# 		        : $operation - cli operation eg. [delete-message,create-queue,...]
# 		        : $params - anonymous hash of option/argument pairs
# 		          $params = { 'option' => '$argument', 'queue-url' => '$q' }
# RETURN	        : $res - anonymous hash of the response or undef
sub api_call {
	my ($self, $service, $operation, $params) = @_;
	my $res = $self->{_aws}->$service($operation, $params);
	if ($res) { return $res }
	else 
	{
		warn $AWS::CLIWrapper::Error->{Code};
		warn $AWS::CLIWrapper::Error->{Message};
		return undef;
	}
}

# daemon()              : daemonize process
# ARGS		        : NONE
# RETURN	        : NONE
# STDOUT, STDERR        : /var/log/sqsd{-error}.log
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
	printf("%.3f :: %-12s\n", time, $debug_string);
        print Dumper $debug_string;
}

1;
