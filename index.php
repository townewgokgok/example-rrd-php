<?php
require_once 'vendor/autoload.php';

use Symfony\Component\Yaml\Yaml;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Channel\AMQPChannel;

class SettingsAmqp {
	/** @var string */
	public $host;
	/** @var int */
	public $port;
	/** @var string */
	public $user;
	/** @var string */
	public $pass;
	/** @var string */
	public $queue;
}

class SettingsRrd {
	/** @var string */
	public $file_path_fmt;
	/** @var int */
	public $step;
	/** @var int */
	public $heartbeat;
}

class Settings {
	/** @var SettingsAmqp */
	public $amqp;
	/** @var SettingsRrd */
	public $rrd;
}

class RrdRequest {
	/** @var int */
	public $id;
	/** @var int */
	public $at;
	/** @var float[] */
	public $values;
}

/** @var Settings $s */
$s = null;

/** @var int */
$startTime = 0;

/** @var int */
$received = 0;

/** @var int */
$count = 0;

/** @var RrdRequest[] */
$queue = [];

/** @var AMQPChannel */
$ch = null;

function processRequest($req) {
	global $s, $startTime, $count, $ch;
	$path = sprintf($s->rrd->file_path_fmt, $req->id);
	if (!file_exists($path)) {
		printf("Creating RRD file: %s\n", $path);
		rrd_create(
			$path,
			[
				'--start', $req->at - 1,
				'--step', $s->rrd->step,
				"DS:value1:GAUGE:" . $s->rrd->heartbeat . ":U:U",
				"DS:value2:GAUGE:" . $s->rrd->heartbeat . ":U:U",
				"DS:value3:GAUGE:" . $s->rrd->heartbeat . ":U:U",
				"RRA:AVERAGE:0.5:1:3600",
			]
		);
	}
	$args = $req->values;
	array_unshift($args, $req->at);
	// printf("Updating RRD file: %s @ %s\n", $path, $req->at);
	for ($i=0; $i<60; $i++) {
		$args[0] = $req->at + $i;
		rrd_update($path, [implode(":", $args)]);
	}

	printf("%d: Sending ack id=%s\n", ++$count, $req->message_id);
	$ch->basic_ack($req->delivery_tag);

	$dt = microtime(true) - $startTime;
	printf("%d: %f [sec]\n", $count, $dt);
}

function onReceive(AMQPMessage $msg) {
	global $startTime, $received, $queue;
	if ($startTime == 0) $startTime = microtime(true);
	/** @var RrdRequest $req */
	$req = json_decode($msg->body);
	$req->delivery_tag = $msg->delivery_info['delivery_tag'];
	$props = $msg->get_properties();
	$req->message_id = $props['message_id'];
	printf("%d: Received a message id=%s\n", ++$received, $req->message_id);
	processRequest($req);
}

function main() {
	global $s, $ch;
	$yamlSrc = file_get_contents("settings.yml");
	$s = Yaml::parse($yamlSrc, Yaml::PARSE_OBJECT_FOR_MAP);

	printf(
		"Connecting to amqp://%s@%s:%d\n",
		$s->amqp->user,
		$s->amqp->host,
		$s->amqp->port ?: 5672
	);
	$conn = new AMQPStreamConnection(
		$s->amqp->host,
		$s->amqp->port ?: 5672,
		$s->amqp->user,
		$s->amqp->pass
	);
	$ch = $conn->channel();

	printf("Waiting for messages\n");
	$ch->basic_consume(
		$s->amqp->queue, //queue
		"",              //consumer_tag
		false,           //no_local
		false,           //no_ack
		false,           //exclusive
		false,           //nowait
		"onReceive"      //callback
	);

	while(count($ch->callbacks)) $ch->wait();
}

main();
