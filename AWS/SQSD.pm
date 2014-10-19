package AWS::SQSD;

use POSIX qw(setsid);
use AWS::CLIWrapper;
use strict;
use warnings;

# Functions for the SQS Daemon
# lokerd@amazon.com

# AWS::SQSD->new()		: constructor
# AWS::SQSD->get_message()	: retrieve from queue
# AWS::SQSD->process()		: process queue message
# AWS::SQSD->api_call()		: API call via AWS::CLIWrapper

use constant {
	REGION          => 'ap-southeast-2',
	Q 	        => 'sme'
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
# OBJECT	: $self->{_queue}  - name of SQS Queue	: CONSTANT
# 		: $self->{_region} - AWS Region		: CONSTANT
sub new {
	my ($class, $args) = @_;
	my $self = {
		queue => +Q,
		region => +REGION,
		args => $args
	};
	bless ($self,$class);
	$self->_init;
	return $self; 
}

# Initialise 	: _init()
# ARGS:		: NONE
# RETURN:	: NONE
# OBJECT	: $self->{_aws} - AWS::CLIWrapper object
# 		: $self->{_q_url} - full URL of SQS Queue
# 		  returned from &api_call(sqs,create-queue...') 
sub _init {
	my $self = shift;
	my $res;
	eval 
	{
		$self->{ _aws } = AWS::CLIWrapper->new(region => $self->{region});
		$res = $self->api_call('sqs', 'create-queue', {
				'queue-name' => $self->{queue}
		});
	}; 
	if ($res) { $self->{'_q_url'} = $res->{QueueUrl} }
	else { die $@ } 
	$self->daemon();
	return $self;
}   

# MSG Worker	: get_message()
# ARGS 		: NONE
# RETURN	: anonymous hash $message
# 		: undef if no $message
# OBJECT	: NONE
sub get_message {
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
# OBJECT	: $self-{_q_url}    - full URL of SQS Queue
# 		: $self->api_call() - API Worker
sub process {
	my ($self, $message) = @_;
	print "Processing $message->{MessageId}\n";
	$self->api_call('sqs', 'delete-message', {
			'queue-url' => $self->{_q_url},
			'receipt-handle' => $message->{ReceiptHandle} 
			});
	return;
}

# API Worker	: api_call(str,str,{})
# ARGS:		: $service - aws cli service eg. [sqs,autoscaling,sns...]
# 		: $operation - cli operation eg. [delete-message,create-queue,...]
# 		: $params - anonymous hash of option/argument pairs
# 		  $params = { 'option' => '$argument', 'queue-url' => '$q' }
# RETURN:	: $res - anonymous hash of the response
# 		: undef if no $res
# OBJECT	: $self-{_aws} - AWS::CLIWrapper object
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
