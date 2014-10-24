package AWS::SQSD;

use POSIX qw(setsid);
use AWS::CLIWrapper;
use LWP::UserAgent;
use LWP::Protocol::https;
use HTTP::Request::Common;
use JSON;
use strict;
use warnings;

use Data::Dumper;

# Functions for the SQS Daemon
# lokerd@amazon.com

# AWS::SQSD->new()		: constructor
# AWS::SQSD->get_message()	: retrieve from queue
# AWS::SQSD->process()		: process queue message
# AWS::SQSD->api_call()		: API call via AWS::CLIWrapper

# Constants
use constant {
	REGION          => 'ap-southeast-2',
	Q 	        => 'worker3-Queue-SZCNQRDGE5VS',
	ALLOC		=> 'eipalloc-63b5a501',
	TARGET		=> 'i-bc8f8d73'
};

# Debug		: debug(string)
# ARGS		: $debug_string - string to be printed
# RETURNS	: NONE
# OBJECT	: NONE	
sub debug {
	my $debug_string = shift;
	printf("%.3f :: %-12s\n", time, $debug_string);
}

# Constructor 	: new({})
# ARGS		: anonymous hash of constructor args - placeholder
# RETURN	: blessed reference 
sub new 
{
	my ($class, $args) = @_;
	my $self = {
		queue => +Q,
		region => +REGION,
		eip_status => 'false',
		eip_assoc => "",
		args => $args
	};
	bless ($self,$class);
	$self->_init;
	return $self; 
}

# Initialise 	: _init()
# ARGS:		: NONE
# RETURN:	: NONE
# SETS		: $self->{_aws} - AWS::CLIWrapper object
# 		: $self->{_ua} - LWP object
# 		: $self->{_q_url} - full URL of SQS Queue
# 		  returned from &api_call(sqs,create-queue...') 
sub _init 
{
	my $self = shift;
	my $res;
	eval 
	{
		$self->{_aws} = AWS::CLIWrapper->new(region => $self->{region});
		$self->{_ua} = LWP::UserAgent->new();
		$res = $self->api_call('sqs', 'create-queue', {
				'queue-name' => $self->{queue}
		});
	}; 
	if ($res) { $self->{'_q_url'} = $res->{QueueUrl} }
	else { die $@ } 
#	$self->daemon();
	return $self;
}   

# MSG Worker	: get_message()
# ARGS 		: NONE
# RETURN	: anonymous hash $message
# 		: undef if no $message
sub get_message 
{
	my $self = shift;
	my $message = $self->api_call('sqs', 'receive-message', {
			'queue-url' => $self->{_q_url}
	});
	if ($message) { return $message }
	else { return undef }
}

# MSG Processor	: process({})
# ARGS		: $message - anonymous message hash
# 		  returned by AWS::SQSD->get_message()  
# RETURN	: NONE
sub process 
{
	my ($self, $message) = @_;
    	my $tmp = decode_json $message;
	my $body = decode_json $tmp->{Message};
	my $response_url = $body->{ResponseURL};

print Dumper $body;
	my $response = {
		'RequestId' => $body->{RequestId},
		'StackId' => $body->{StackId},
		'LogicalResourceId' => $body->{LogicalResourceId}
	};

	if ( $body->{ResourceProperties}{Attach} eq $self->{eip_status} ) 
	{
		$response->{Status} = 'SUCCESS';
		$response->{Data}{AttachmentStatus} = $self->{eip_status};
		my $res = $self->s3_signal($response,$response_url);
	} 
	else
	{
		if ($self->{eip_status} eq 'false') 
		{ 
			my $message = $self->api_call('ec2', 'associate-address', {
				'instance-id' => +TARGET,
				'allocation-id' => +ALLOC
			});
			if($message->{AssociationId}) 
			{
				$self->{eip_status} = 'true';
				$self->{eip_assoc} = $message->{AssociationId};
				$response->{Status} = 'SUCCESS';
				$response->{Data}{AttachmentStatus} = $self->{eip_status};
				$self->s3_signal($response,$response_url);
			} 
			else 
			{
				$response->{Status} = 'FAILED';
				$response->{Data}{AttachmentStatus} = $self->{eip_status};
				$self->s3_signal($response,$response_url);
			}

		} 
		else 
		{
			my $message = $self->api_call('ec2', 'disassociate-address', {
				'instance-id' => +TARGET,
				'assocation-id' => $self->{eip_assoc}
			});
			if($message->{return} eq 'true') 
			{
				$self->{eip_status} = 'false';
				$self->{eip_assoc} = "";
				$response->{Status} = 'SUCCESS';
				$response->{Data}{AttachmentStatus} = $self->{eip_status};
				$self->s3_signal($response,$response_url);
			} 
			else 
			{
				$response->{Status} = 'FAILED';
				$response->{Data}{AttachmentStatus} = $self->{eip_status};
				$self->s3_signal($response, $response_url);
			}
		}
	}	
				
		
	
#	$self->api_call('sqs', 'delete-message', {
#			'queue-url' => $self->{_q_url},
#			'receipt-handle' => $message->{ReceiptHandle} 
#			});
	return;
}

sub s3_signal
{
	my ($self, $response, $response_url) = @_;
	print Dumper $response;
	my $json = encode_json($response);
	my $req = POST $response_url;
	$req->header( 'Content-Type' => 'application/json' );
	$req->content( $json );
	my $res = $self->{_ua}->request($req);
	if ($res) { return }
	else { return undef }
}

# API Worker	: api_call(str,str,{})
# ARGS:		: $service - aws cli service eg. [sqs,autoscaling,sns...]
# 		: $operation - cli operation eg. [delete-message,create-queue,...]
# 		: $params - anonymous hash of option/argument pairs
# 		  $params = { 'option' => '$argument', 'queue-url' => '$q' }
# RETURN:	: $res - anonymous hash of the response
# 		: undef if no $res
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

# Daemonize	: daemon()
# ARGS		: NONE
# RETURN	: NONE
# OBJECT	: NONE
# STDIN 	: DETACHED
# STDOUT, STDERR: /var/log/sqsd{-error}.log
sub daemon {
	chdir '/'                 or die "Can't chdir to /: $!";
	open STDIN, '/dev/null'   or die "Can't read /dev/null: $!";
	open STDOUT, '>>/var/log/sqsd.log' or die "Can't write to /dev/null: $!";
	open STDERR, '>>/var/log/sqsd-error.log' or die "Can't write to /dev/null: $!";
	defined(my $pid = fork)   or die "Can't fork: $!";
	exit if $pid;
	setsid                    or die "Can't start a new session: $!";
	umask 0;
}

1;
